/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.dataagent.workflow.node;

import com.alibaba.cloud.ai.dataagent.constant.DocumentMetadataConstant;
import com.alibaba.cloud.ai.dataagent.enums.KnowledgeType;
import com.alibaba.cloud.ai.dataagent.enums.TextType;
import com.alibaba.cloud.ai.dataagent.dto.prompt.EvidenceQueryRewriteDTO;
import com.alibaba.cloud.ai.dataagent.entity.AgentKnowledge;
import com.alibaba.cloud.ai.dataagent.mapper.AgentKnowledgeMapper;
import com.alibaba.cloud.ai.dataagent.prompt.PromptHelper;
import com.alibaba.cloud.ai.dataagent.service.llm.LlmService;
import com.alibaba.cloud.ai.dataagent.service.vectorstore.AgentVectorStoreService;
import com.alibaba.cloud.ai.dataagent.util.*;
import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.action.NodeAction;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.document.Document;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.*;

/**
 * 证据召回节点 —— StateGraph 工作流中负责从知识库检索相关证据的节点。
 *
 * <h3>在工作流中的位置</h3>
 * <p>IntentRecognition → <b>EvidenceRecallNode（当前节点）</b> → QueryEnhance → SchemaRecall → TableRelation → ...</p>
 *
 * <h3>核心职责</h3>
 * <p>从业务知识库和智能体知识库中检索与用户问题相关的文档片段（称为"证据"），
 *    作为后续 SQL 生成和数据分析的上下文依据。</p>
 *
 * <h3>涉及的 AI 概念</h3>
 * <ul>
 *   <li><b>RAG（Retrieval-Augmented Generation，检索增强生成）</b>：
 *       核心思想是在让 LLM 生成 SQL 之前，先从外部知识库中检索相关信息，
 *       把检索结果作为 Prompt 的一部分提供给 LLM，从而让 LLM 的回答基于事实而非凭空编造。
 *       本节点实现了 RAG 的 "Retrieval" 阶段。</li>
 *   <li><b>Query Rewrite（查询重写）</b>：
 *       用户原始提问往往包含省略、指代或口语化表达，不适合直接用于检索。
 *       先让 LLM 将其改写为一个独立、完整的检索语句（standalone query），提高检索召回率。</li>
 *   <li><b>Embedding / 向量检索</b>：
 *       知识库中的文档在入库时已经过 Embedding 模型转换为高维向量（如 1536 维）。
 *       检索时将改写后的查询也转为向量，在向量空间中计算余弦相似度，返回最相关的 TopK 文档。
 *       这一步由 {@link AgentVectorStoreService} 封装完成。</li>
 *   <li><b>Hybrid Search（混合检索，可选）</b>：
 *       向量语义检索 + 关键词精确匹配的融合策略，提高召回准确率。</li>
 * </ul>
 *
 * <h3>处理流程（两阶段 RAG）</h3>
 * <ol>
 *   <li><b>Phase 1 - 查询重写</b>：LLM 将用户提问改写为标准检索语句</li>
 *   <li><b>Phase 2 - 向量检索</b>：使用改写后的语句在 VectorStore 中做语义检索，
 *       分别检索"业务术语"和"智能体知识"两类文档</li>
 * </ol>
 *
 * @see AgentVectorStoreService 向量存储检索服务
 * @see LlmService LLM 调用服务（用于查询重写）
 * @see EvidenceQueryRewriteDTO 查询重写结果 DTO
 */
@Slf4j
@Component
@AllArgsConstructor
public class EvidenceRecallNode implements NodeAction {

	private final LlmService llmService;

	private final AgentVectorStoreService vectorStoreService;

	private final JsonParseUtil jsonParseUtil;

	private final AgentKnowledgeMapper agentKnowledgeMapper;

	/**
	 * 节点执行入口，由 StateGraph 框架在到达该节点时自动调用。
	 *
	 * <h3>输入状态读取</h3>
	 * <ul>
	 *   <li>{@code INPUT_KEY} —— 用户原始提问</li>
	 *   <li>{@code AGENT_ID} —— 当前智能体 ID，用于限定知识库检索范围</li>
	 *   <li>{@code MULTI_TURN_CONTEXT} —— 多轮对话上下文</li>
	 * </ul>
	 *
	 * <h3>LLM 调用逻辑</h3>
	 * <p>调用 LLM 对用户提问进行"查询重写"（Query Rewrite），将口语化的提问
	 *    转换为适合向量检索的标准语句。注意：此处不做多子查询扩展，
	 *    因为 LLM 在此阶段不了解公司特有的业务术语（如 PV、GMV 等），盲目扩展反而引入噪音。</p>
	 *
	 * <h3>向量检索逻辑</h3>
	 * <p>LLM 查询重写完成后（在 resultMapper 回调中），使用改写后的 standalone query
	 *    在向量数据库中分别检索"业务术语"和"智能体知识"两类文档。</p>
	 *
	 * <h3>输出状态写入</h3>
	 * <ul>
	 *   <li>{@code EVIDENCE} —— 格式化后的证据文本，供后续 Planner/SQL 生成节点作为 Prompt 上下文</li>
	 * </ul>
	 *
	 * @param state StateGraph 的全局共享状态
	 * @return 包含流式生成器的 Map，key 为状态字段名
	 */
	@Override
	public Map<String, Object> apply(OverAllState state) throws Exception {

		// 从全局状态中读取用户提问和当前智能体 ID
		String question = StateUtil.getStringValue(state, INPUT_KEY);
		String agentId = StateUtil.getStringValue(state, AGENT_ID);
		Assert.hasText(agentId, "Agent ID cannot be empty.");

		log.info("Rewriting query before getting evidence in question: {}", question);
		log.debug("Agent ID: {}", agentId);

		String multiTurn = StateUtil.getStringValue(state, MULTI_TURN_CONTEXT, "(无)");

		// 构建查询重写的 Prompt。
		// 设计决策：只做单查询重写，不做子查询扩展。
		// 原因是此时 LLM 还不了解公司特有的业务术语（如 PV, GMV, DAU 等），
		// 扩展反而会引入不相关的检索词，降低检索精度。
		String prompt = PromptHelper.buildEvidenceQueryRewritePrompt(multiTurn, question);
		log.debug("Built evidence-query-rewrite prompt as follows \n {} \n", prompt);

		// 以流式模式调用 LLM 进行查询重写，返回 Flux<ChatResponse>
		Flux<ChatResponse> responseFlux = llmService.callUser(prompt);

		// Sinks.Many 是 Reactor 的手动信号发射器，用于将向量检索的进度信息推送到前端。
		// multicast().onBackpressureBuffer() 表示支持多个订阅者，并在消费速度跟不上时缓冲数据。
		// 这里用它将 Phase 2（向量检索）的进度信息实时推送给前端，与 Phase 1（LLM 查询重写）串联展示。
		Sinks.Many<String> evidenceDisplaySink = Sinks.many().multicast().onBackpressureBuffer();

		// RAG 两阶段设计：
		// Phase 1（本 generator）：LLM 改写查询为标准检索语句，流式输出重写过程
		// Phase 2（在 resultMapper 回调中触发）：用改写结果在 VectorStore 中做向量语义检索
		final Map<String, Object> resultMap = new HashMap<>();
		Flux<GraphResponse<StreamingOutput>> generator = FluxUtil.createStreamingGenerator(this.getClass(), state,
				responseFlux,
				Flux.just(ChatResponseUtil.createResponse("正在查询重写以更好召回evidence..."),
						ChatResponseUtil.createPureResponse(TextType.JSON.getStartSign())),
				Flux.just(ChatResponseUtil.createPureResponse(TextType.JSON.getEndSign()),
						ChatResponseUtil.createResponse("\n查询重写完成！")),
				result -> {
					// resultMapper 回调：LLM 完整输出到达后触发。
					// 在这里执行 Phase 2：解析重写结果，进行向量检索
					resultMap.putAll(getEvidences(result, agentId, evidenceDisplaySink));
					return resultMap;
				});

		// evidenceDisplaySink 发射的进度消息（如"正在获取证据..."、"已找到 N 条相关证据"等）
		// 通过这个 evidenceFlux 流式推送到前端。
		// Flux.empty() 作为 preFlux 和 postFlux，表示不需要额外的前后置消息。
		Flux<GraphResponse<StreamingOutput>> evidenceFlux = FluxUtil.createStreamingGenerator(this.getClass(), state,
				evidenceDisplaySink.asFlux().map(ChatResponseUtil::createPureResponse), Flux.empty(), Flux.empty(),
				result -> resultMap);

		// generator.concatWith(evidenceFlux) 将两个 Flux 串联：
		// 先执行 Phase 1（LLM 查询重写），完成后接续 Phase 2（向量检索进度推送）
		return Map.of(EVIDENCE, generator.concatWith(evidenceFlux));
	}

	/**
	 * RAG Phase 2：解析 LLM 查询重写结果，执行向量检索并组装证据。
	 *
	 * @param llmOutput LLM 的完整输出文本，包含 JSON 格式的查询重写结果
	 * @param agentId   当前智能体 ID，用于限定检索范围
	 * @param sink      进度信息发射器，向前端推送检索进度
	 * @return 包含 EVIDENCE 键的 Map，值为格式化后的证据文本
	 */
	private Map<String, Object> getEvidences(String llmOutput, String agentId, Sinks.Many<String> sink) {
		try {
			// 从 LLM 输出中提取 standalone_query 字段
			String standaloneQuery = extractStandaloneQuery(llmOutput);

			if (null == standaloneQuery || standaloneQuery.isEmpty()) {
				log.debug("No standalone query from LLM output");
				sink.tryEmitNext("未能进行查询重写！\n");
				return Map.of(EVIDENCE, "无");
			}

			// 向前端推送重写后的查询，让用户看到系统如何理解他的问题
			outputRewrittenQuery(standaloneQuery, sink);

			// 执行向量检索：分别在"业务术语"和"智能体知识"两个向量空间中检索
			// 底层会使用 Embedding 模型将 standaloneQuery 转为向量，然后计算余弦相似度
			DocumentRetrievalResult retrievalResult = retrieveDocuments(agentId, standaloneQuery);

			// 检查是否有证据文档
			if (retrievalResult.allDocuments().isEmpty()) {
				log.debug("No evidence documents found for agent: {} with query: {}", agentId, standaloneQuery);
				sink.tryEmitNext("未找到证据！\n");
				return Map.of(EVIDENCE, "无");
			}

			// 将检索到的文档按类型格式化为文本，作为后续 LLM Prompt 的一部分
			String evidence = buildFormattedEvidenceContent(retrievalResult.businessTermDocuments(),
					retrievalResult.agentKnowledgeDocuments());
			log.info("Evidence content built as follows \n {} \n", evidence);
			// 向前端展示证据摘要
			outputEvidenceContent(retrievalResult.allDocuments(), sink);

			return Map.of(EVIDENCE, evidence);
		}
		catch (Exception e) {
			log.error("Error occurred while getting evidences", e);
			sink.tryEmitError(e);
			return Map.of(EVIDENCE, "");
		}
		finally {
			// 发射完成信号，关闭 sink，前端流结束
			sink.tryEmitComplete();
		}
	}

	private void outputRewrittenQuery(String standaloneQuery, Sinks.Many<String> sink) {
		sink.tryEmitNext("重写后查询：\n");
		sink.tryEmitNext(standaloneQuery + "\n");
		log.debug("Using standalone query for evidence recall: {}", standaloneQuery);
		sink.tryEmitNext("正在获取证据...");
	}

	/**
	 * 执行向量检索：分别检索业务术语文档和智能体知识文档。
	 *
	 * <p>检索过程：standalone query → Embedding 模型转为向量 → 在向量空间中按余弦相似度排序 → 返回 TopK 文档。</p>
	 * <p>两类文档使用不同的 metadata 标签区分，共享同一个向量存储空间。</p>
	 *
	 * @param agentId         智能体 ID，用于过滤只属于当前智能体的文档
	 * @param standaloneQuery LLM 重写后的标准检索语句
	 * @return 包含分类和合并结果的检索结果 record
	 */
	private DocumentRetrievalResult retrieveDocuments(String agentId, String standaloneQuery) {
		// 检索"业务术语"类型的文档：如行业术语表、指标定义、数据字典等
		// DocumentMetadataConstant.BUSINESS_TERM 是 metadata 中的类型标签
		List<Document> businessTermDocuments = vectorStoreService
			.getDocumentsForAgent(agentId, standaloneQuery, DocumentMetadataConstant.BUSINESS_TERM)
			.stream()
			.toList();

		// 检索"智能体知识"类型的文档：如 FAQ、QA 对、上传的业务文档等
		// 这些知识是用户为特定智能体配置的，帮助 LLM 理解公司特有的业务逻辑
		List<Document> agentKnowledgeDocuments = vectorStoreService
			.getDocumentsForAgent(agentId, standaloneQuery, DocumentMetadataConstant.AGENT_KNOWLEDGE)
			.stream()
			.toList();

		// 合并所有证据文档
		List<Document> allDocuments = new ArrayList<>();
		if (!businessTermDocuments.isEmpty())
			allDocuments.addAll(businessTermDocuments);
		if (!agentKnowledgeDocuments.isEmpty())
			allDocuments.addAll(agentKnowledgeDocuments);

		// 添加文档检索日志
		log.info("Retrieved documents for agent {}: {} business term docs, {} agent knowledge docs, total {} docs",
				agentId, businessTermDocuments.size(), agentKnowledgeDocuments.size(), allDocuments.size());

		return new DocumentRetrievalResult(businessTermDocuments, agentKnowledgeDocuments, allDocuments);
	}

	/**
	 * 将检索到的文档格式化为证据文本，最终拼接到 LLM 的 Prompt 中。
	 *
	 * <p>输出格式示例：</p>
	 * <pre>
	 * 1. [来源: 2025Q3报告-销售数据.md] ...华东地区的增长主要来自于核心用户...
	 * 2. [来源: 客服FAQ] Q: 退款怎么算? A: 只统计已入库退货...
	 * </pre>
	 *
	 * @param businessTermDocuments    业务术语类文档列表
	 * @param agentKnowledgeDocuments  智能体知识类文档列表
	 * @return 格式化后的证据文本，若两类均为空则返回"无"
	 */
	private String buildFormattedEvidenceContent(List<Document> businessTermDocuments,
			List<Document> agentKnowledgeDocuments) {
		// 构建业务知识内容
		String businessKnowledgeContent = buildBusinessKnowledgeContent(businessTermDocuments);

		// 构建智能体知识内容
		String agentKnowledgeContent = buildAgentKnowledgeContent(agentKnowledgeDocuments);

		// 使用PromptHelper的模板方法进行渲染
		String businessPrompt = PromptHelper.buildBusinessKnowledgePrompt(businessKnowledgeContent);
		String agentPrompt = PromptHelper.buildAgentKnowledgePrompt(agentKnowledgeContent);

		// 添加证据构建日志
		log.info("Building evidence content: business knowledge length {}, agent knowledge length {}",
				businessKnowledgeContent.length(), agentKnowledgeContent.length());

		// 拼接业务知识和智能体知识作为证据
		return businessKnowledgeContent.isEmpty() && agentKnowledgeContent.isEmpty() ? "无"
				: businessPrompt + (agentKnowledgeContent.isEmpty() ? "" : "\n\n" + agentPrompt);
	}

	/**
	 * 构建业务知识文本：将每个业务术语 Document 的内容直接拼接。
	 * 业务术语通常是结构化的定义（如"GMV = 成交总额"），不需要额外格式化。
	 */
	private String buildBusinessKnowledgeContent(List<Document> businessTermDocuments) {
		if (businessTermDocuments.isEmpty()) {
			return "";
		}

		StringBuilder result = new StringBuilder();

		// 直接使用Document的完整内容，每行一个Document
		for (Document doc : businessTermDocuments) {
			result.append(doc.getText()).append("\n");
		}

		return result.toString();
	}

	/**
	 * 构建智能体知识文本：根据知识类型（FAQ/QA vs 文档）分别格式化。
	 *
	 * <p>智能体知识分为两类：</p>
	 * <ul>
	 *   <li>FAQ/QA 类型：格式为 "Q: 问题 A: 答案"，从数据库查询完整内容</li>
	 *   <li>DOCUMENT 类型：从文件上传的知识文档，格式为 "[来源: 标题-文件名] 内容"</li>
	 * </ul>
	 */
	private String buildAgentKnowledgeContent(List<Document> agentKnowledgeDocuments) {
		if (agentKnowledgeDocuments.isEmpty()) {
			return "";
		}

		StringBuilder result = new StringBuilder();

		for (int i = 0; i < agentKnowledgeDocuments.size(); i++) {
			Document doc = agentKnowledgeDocuments.get(i);
			Map<String, Object> metadata = doc.getMetadata();
			// 每个文档的 metadata 中记录了具体的知识子类型（FAQ、QA、DOCUMENT 等）
			String knowledgeType = (String) metadata.get(DocumentMetadataConstant.CONCRETE_AGENT_KNOWLEDGE_TYPE);

			// 根据知识类型调用不同的格式化方法
			if (KnowledgeType.FAQ.getCode().equals(knowledgeType) || KnowledgeType.QA.getCode().equals(knowledgeType)) {
				processFaqOrQaKnowledge(doc, i, result);
			}
			else {
				processDocumentKnowledge(doc, i, result);
			}
		}

		return result.toString();
	}

	/**
	 * 处理 FAQ/QA 类型的知识文档。
	 *
	 * <p>向量检索返回的 Document 只包含问题文本（即向量化的内容），需要通过 metadata 中的
	 * knowledgeId 从数据库查询对应的答案（answer），然后组装为 "Q: 问题 A: 答案" 的格式。</p>
	 *
	 * @param doc   向量检索返回的文档，text 为问题，metadata 包含 knowledgeId
	 * @param index 文档序号，用于输出编号
	 * @param result 拼接结果
	 */
	private void processFaqOrQaKnowledge(Document doc, int index, StringBuilder result) {
		Map<String, Object> metadata = doc.getMetadata();
		String content = doc.getText();
		// 向量存储时，将数据库中的主键 ID 存入 metadata，检索后通过 ID 回查完整内容
		Integer knowledgeId = ((Number) metadata.get(DocumentMetadataConstant.DB_AGENT_KNOWLEDGE_ID)).intValue();
		String knowledgeType = (String) metadata.get(DocumentMetadataConstant.CONCRETE_AGENT_KNOWLEDGE_TYPE);

		log.debug("Processing {} type knowledge with id: {}", knowledgeType, knowledgeId);

		if (knowledgeId != null) {
			try {
				// 从数据库查询完整的知识记录（包含 title 和 answer）
				AgentKnowledge knowledge = agentKnowledgeMapper.selectById(knowledgeId);
				if (knowledge != null) {
					String title = knowledge.getTitle();
					// 格式：序号. [来源: 标题] Q: 问题文本 A: 答案文本
					result.append(index + 1).append(". [来源: ");
					result.append(title.isEmpty() ? "知识库" : title);
					result.append("] Q: ").append(content).append(" A: ").append(knowledge.getContent()).append("\n");

					log.debug("Successfully processed {} knowledge with title: {}", knowledgeType, title);
				}
				else {
					log.warn("Knowledge not found for id: {}", knowledgeId);
				}
			}
			catch (Exception e) {
				log.error("Error getting knowledge by id: {}", knowledgeId, e);
				// 数据库查询失败时降级：直接使用向量检索返回的文本内容
				result.append(index + 1).append(". [来源: 知识库] ").append(content).append("\n");
			}
		}
		else {
			// metadata 中没有 knowledgeId，说明文档入库时缺少关联信息
			log.error("No knowledge id found for agent knowledge document: {}", doc.getId());
			result.append(index + 1).append(". [来源: 知识库] ").append(content).append("\n");
		}
	}

	/**
	 * 处理 DOCUMENT 类型的知识文档（用户上传的文件，如 PDF、Word、Markdown 等）。
	 *
	 * <p>与 FAQ/QA 不同，文档类型的 text 字段已包含切片后的完整内容，
	 * 只需从 metadata 获取来源文件名用于标注。</p>
	 *
	 * @param doc   向量检索返回的文档
	 * @param index 文档序号
	 * @param result 拼接结果
	 */
	private void processDocumentKnowledge(Document doc, int index, StringBuilder result) {
		Map<String, Object> metadata = doc.getMetadata();
		String content = doc.getText();
		Integer knowledgeId = ((Number) metadata.get(DocumentMetadataConstant.DB_AGENT_KNOWLEDGE_ID)).intValue();
		String knowledgeType = (String) metadata.get(DocumentMetadataConstant.CONCRETE_AGENT_KNOWLEDGE_TYPE);
		String title = "";
		String sourceFilename = "";

		log.debug("Processing {} type knowledge with id: {}", knowledgeType, knowledgeId);

		if (knowledgeId != null) {
			try {
				AgentKnowledge knowledge = agentKnowledgeMapper.selectById(knowledgeId);
				if (knowledge != null) {
					title = knowledge.getTitle();
					sourceFilename = knowledge.getSourceFilename();

					log.debug("Successfully processed {} knowledge with title: {}, source file: {}", knowledgeType,
							title, sourceFilename);
				}
				else {
					log.warn("Knowledge not found for id: {}", knowledgeId);
				}
			}
			catch (Exception e) {
				log.error("Error getting knowledge by id: {}", knowledgeId, e);
			}
		}

		// 构建来源信息，格式为"标题-文件名"
		String sourceInfo = title.isEmpty() ? "文档" : title;
		if (!sourceFilename.isEmpty()) {
			sourceInfo += "-" + sourceFilename;
		}

		result.append(index + 1).append(". [来源: ");
		result.append(sourceInfo);
		result.append("] ").append(content).append("\n");
	}

	/**
	 * 向前端推送证据文档的摘要信息。
	 * 每个文档最多显示前 100 个字符，避免流式传输大量文本影响体验。
	 */
	private void outputEvidenceContent(List<Document> allDocuments, Sinks.Many<String> sink) {
		if (allDocuments.isEmpty()) {
			return;
		}

		log.info("Outputting evidence content for {} documents", allDocuments.size());
		sink.tryEmitNext("已找到 " + allDocuments.size() + " 条相关证据文档，如下是文档的部分信息\n");

		// 只输出文档的摘要信息，而不是完整内容
		for (int i = 0; i < allDocuments.size(); i++) {
			Document doc = allDocuments.get(i);
			String content = doc.getText();

			// 限制每个文档摘要的长度，最多显示100个字符
			String summary = content.length() > 100 ? content.substring(0, 100) + "..." : content;

			sink.tryEmitNext(String.format("证据%d: %s\n", i + 1, summary));
		}
	}

	private record DocumentRetrievalResult(List<Document> businessTermDocuments, List<Document> agentKnowledgeDocuments,
			List<Document> allDocuments) {
	}

	/**
	 * 从 LLM 输出中解析 standalone_query 字段。
	 *
	 * <p>LLM 返回的是 Markdown 包裹的 JSON（如 ```json\n{...}\n```），
	 * 先用 MarkdownParserUtil 提取纯 JSON，再用 Jackson 反序列化为 DTO。</p>
	 *
	 * @param llmOutput LLM 的原始输出文本
	 * @return 重写后的独立查询语句，解析失败返回 null
	 */
	private String extractStandaloneQuery(String llmOutput) {
		EvidenceQueryRewriteDTO evidenceQueryRewriteDTO;
		try {
			String content = MarkdownParserUtil.extractText(llmOutput.trim());
			evidenceQueryRewriteDTO = jsonParseUtil.tryConvertToObject(content, EvidenceQueryRewriteDTO.class);
			log.info("For getting evidence, successfully parsed EvidenceQueryRewriteDTO from LLM response: {}",
					evidenceQueryRewriteDTO);
			return evidenceQueryRewriteDTO.getStandaloneQuery();
		}
		catch (Exception e) {
			log.error("Failed to parse EvidenceQueryRewriteDTO from LLM response", e);
		}
		return null;
	}

}

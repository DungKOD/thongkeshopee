import { invoke } from "./tauri";

export interface AiValidateResult {
  valid: boolean;
  modelsCount: number;
  errorMsg: string | null;
  /** Danh sách model chat-capable (gpt-*, o1/o3/o4, chatgpt-*), sort desc. */
  models: string[];
}

export interface AiGenerateParams {
  apiKey: string;
  model: string;
  productName: string;
  shopName?: string;
  price?: number;
  sales?: number;
}

/** Check API key OpenAI bằng cách GET /v1/models. Không throw — trả flag. */
export function validateOpenAiKey(apiKey: string): Promise<AiValidateResult> {
  return invoke<AiValidateResult>("ai_validate_openai_key", { apiKey });
}

/**
 * Sinh content FB ads tiếng Việt cho 1 sản phẩm Shopee. AI tự phân tích
 * ngành hàng từ tên SP → chọn emoji + giọng văn phù hợp. Throw khi network
 * fail / OpenAI 4xx-5xx — caller hiện retry button.
 */
export function generateFbContent(params: AiGenerateParams): Promise<string> {
  return invoke<string>("ai_generate_fb_content", {
    params: {
      api_key: params.apiKey,
      model: params.model,
      product_name: params.productName,
      shop_name: params.shopName ?? "",
      price: params.price ?? 0,
      sales: params.sales ?? 0,
    },
  });
}

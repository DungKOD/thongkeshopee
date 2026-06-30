//! OpenAI integration cho tính năng "AI tạo content FB" trong Shopee Product.
//!
//! 2 command:
//! - `ai_validate_openai_key`: GET `/v1/models` Bearer auth → check 200 OK, đếm model.
//! - `ai_generate_fb_content`: POST `/v1/chat/completions` với prompt copywriting
//!   FB ads tiếng Việt; AI tự phân tích ngành hàng từ tên SP rồi chọn emoji +
//!   giọng văn phù hợp; output cấu trúc cố định (~12-15 dòng), link Shopee
//!   chừa trống để user paste affiliate link sau.
//!
//! Không cache response — moderate cost (~$0.001/SP với gpt-4o-mini), user
//! kiểm soát qua nút "Tạo lại" + toggle Enabled. Timeout 60s vì copywriting
//! response có thể dài.

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{CmdError, CmdResult};

const OPENAI_BASE: &str = "https://api.openai.com/v1";
const VALIDATE_TIMEOUT_SECS: u64 = 15;
const GENERATE_TIMEOUT_SECS: u64 = 60;

fn http_client(timeout_secs: u64) -> Result<reqwest::Client, CmdError> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .build()
        .map_err(CmdError::from)
}

/// Kết quả validate API key + list chat models để FE populate dropdown.
/// `modelsCount` = tổng số model API trả về (raw, gồm cả embedding/whisper/...);
/// `models` = subset đã lọc các model có khả năng chat completion (gpt-*,
/// o1/o3/o4, chatgpt-*), sort desc theo id để model mới nhất lên đầu.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AiValidateResult {
    pub valid: bool,
    pub models_count: usize,
    pub error_msg: Option<String>,
    pub models: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ModelsList {
    #[serde(default)]
    data: Vec<ModelEntry>,
}

#[derive(Debug, Deserialize)]
struct ModelEntry {
    #[serde(default)]
    id: String,
}

/// Lọc model có khả năng làm chat completion. OpenAI `/v1/models` trả tất tần
/// tật (embedding, whisper, tts, dall-e, moderation, ...) — phải lọc để
/// dropdown chỉ hiện model dùng được cho chat. Heuristic dựa vào tiền tố +
/// các keyword loại trừ rõ ràng.
fn is_chat_model(id: &str) -> bool {
    let l = id.to_lowercase();
    // Exclude phổ biến — substring match.
    const EXCLUDED_SUBSTR: &[&str] = &[
        "embed",
        "whisper",
        "tts",
        "audio",
        "realtime",
        "transcribe",
        "moderation",
        "image",
        "search",
    ];
    if EXCLUDED_SUBSTR.iter().any(|kw| l.contains(kw)) {
        return false;
    }
    // Exclude legacy / non-chat hoàn toàn — prefix match.
    const EXCLUDED_PREFIX: &[&str] = &["dall-e", "babbage", "davinci", "text-", "code-"];
    if EXCLUDED_PREFIX.iter().any(|p| l.starts_with(p)) {
        return false;
    }
    // Include — prefix các family chat.
    const INCLUDED_PREFIX: &[&str] = &["gpt-", "chatgpt", "o1", "o3", "o4"];
    INCLUDED_PREFIX.iter().any(|p| l.starts_with(p))
}

/// Validate OpenAI API key bằng cách GET `/v1/models`. Endpoint này nhẹ
/// (chỉ list metadata) và chính xác cho việc check key + permission. Không
/// throw error khi key invalid — trả `valid=false` + `error_msg` để UI render
/// inline thay vì toast.
#[tauri::command]
pub async fn ai_validate_openai_key(api_key: String) -> CmdResult<AiValidateResult> {
    let trimmed = api_key.trim();
    if trimmed.is_empty() {
        return Ok(AiValidateResult {
            valid: false,
            models_count: 0,
            error_msg: Some("API key trống".into()),
            models: vec![],
        });
    }
    if !trimmed.starts_with("sk-") {
        return Ok(AiValidateResult {
            valid: false,
            models_count: 0,
            error_msg: Some("API key OpenAI phải bắt đầu bằng 'sk-'".into()),
            models: vec![],
        });
    }

    let client = http_client(VALIDATE_TIMEOUT_SECS)?;
    let resp = client
        .get(format!("{OPENAI_BASE}/models"))
        .bearer_auth(trimmed)
        .send()
        .await
        .map_err(|e| CmdError::msg(format!("Lỗi kết nối OpenAI: {e}")))?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| CmdError::msg(format!("Lỗi đọc response: {e}")))?;

    if status == reqwest::StatusCode::UNAUTHORIZED {
        return Ok(AiValidateResult {
            valid: false,
            models_count: 0,
            error_msg: Some("API key không hợp lệ hoặc đã bị thu hồi".into()),
            models: vec![],
        });
    }
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Ok(AiValidateResult {
            valid: false,
            models_count: 0,
            error_msg: Some("Key bị rate-limit (429). Thử lại sau ít phút.".into()),
            models: vec![],
        });
    }
    if !status.is_success() {
        let preview = &text[..text.len().min(200)];
        return Ok(AiValidateResult {
            valid: false,
            models_count: 0,
            error_msg: Some(format!("HTTP {status}: {preview}")),
            models: vec![],
        });
    }

    let parsed: ModelsList = serde_json::from_str(&text).unwrap_or(ModelsList { data: vec![] });
    let raw_count = parsed.data.len();
    let mut models: Vec<String> = parsed
        .data
        .into_iter()
        .map(|m| m.id)
        .filter(|id| is_chat_model(id))
        .collect();
    // Sort desc theo id: model mới nhất (gpt-5, gpt-4o-mini, ...) lên đầu.
    // Lexicographic OK vì OpenAI versioning follow naming với số version trong id.
    models.sort_by(|a, b| b.cmp(a));
    models.dedup();
    Ok(AiValidateResult {
        valid: true,
        models_count: raw_count,
        error_msg: None,
        models,
    })
}

#[derive(Debug, Deserialize)]
pub struct GenerateParams {
    pub api_key: String,
    pub model: String,
    pub product_name: String,
    #[serde(default)]
    pub shop_name: String,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub sales: i64,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    #[serde(default)]
    choices: Vec<ChatChoice>,
    #[serde(default)]
    error: Option<OpenAiError>,
}

#[derive(Debug, Deserialize)]
struct OpenAiError {
    message: String,
    #[serde(default)]
    code: Option<String>,
}

/// Prompt hệ thống. Mục tiêu: AI tự phân loại ngành hàng từ tên SP, chọn
/// emoji + giọng văn cho phù hợp (food vs fashion vs electronics vs home...),
/// rồi đổ vào cấu trúc cố định để output predictable. Cấm các từ trigger
/// spam FB (mua/đặt/sale/khuyến mãi/giảm giá), tránh phóng đại để vượt qua
/// review của FB.
fn system_prompt() -> &'static str {
    r#"Bạn là copywriter Facebook Ads chuyên viết content quảng cáo affiliate Shopee tiếng Việt.
Mục tiêu: tạo bài viết hấp dẫn người đọc click vào link Shopee để xem & mua hàng, có chuyển đổi cao.

QUY TẮC BẮT BUỘC:

1. PHÂN TÍCH NGÀNH HÀNG: Đọc tên SP, tự xác định ngành hàng (thời trang, mỹ phẩm, đồ ăn, đồ gia dụng, điện tử, đồ chơi, đồ dùng cá nhân, mẹ & bé, văn phòng phẩm, thể thao...), CHỌN emoji + giọng văn phù hợp với ngành đó:
   - Thời trang nữ: 🌸🍒💕✨ (nhẹ nhàng, gợi cảm giác xinh xắn)
   - Đồ gia dụng/nhà bếp: 🍖🥗🥕🏡🍳 (thực tế, tiện lợi, tiết kiệm thời gian)
   - Điện tử/gadget: ⚡🔥📱💻🎮 (mạnh mẽ, tiện ích, công nghệ)
   - Mỹ phẩm: 🌷🌸💄✨🦋 (chăm sóc, làm đẹp, tự tin)
   - Đồ ăn/thực phẩm: 🍓🍪🍰🥤😋 (ngon miệng, an toàn)
   - Mẹ & bé: 🍼👶🌈💗 (an toàn, mềm mại, yêu thương)

2. CẤU TRÚC OUTPUT (12-15 dòng, mỗi dòng 1 ý):
Dòng 1: `🍒Xem mẫu : ` (CHỪA TRỐNG SAU DẤU `:`, KHÔNG được tự sinh link)
Dòng 2: TITLE viết hoa + 2 emoji 2 bên + slogan ngắn gọn (vd: `🥕 MÁY XAY ĐA NĂNG – NHANH, GỌN, TIỆN 🥩`)
Dòng 3-7: 5 dòng `✨` bullet đặc điểm/tính năng SP (chất liệu, thiết kế, công năng cụ thể)
Dòng 8: 1 dòng emoji ngành (🍖/👗/⚡...) — use case (dùng cho việc gì / dịp gì)
Dòng 9: 1 dòng emoji (🥗/💼/⭐...) — lợi ích (tiết kiệm, tiện lợi, nâng tầm phong cách)
Dòng 10: 1 dòng 🏡 — phù hợp với ai/đâu (gia đình, văn phòng, đi học, đi chơi...)
Dòng 11: 1 dòng 💖 — thông điệp cảm xúc (trợ thủ đắc lực, người bạn đồng hành...)
Dòng 12: 1 dòng 📦 — chất lượng/độ bền (thiết kế bền đẹp, vật liệu cao cấp...)
Dòng 13: 1 dòng 📸 — hình thật / thao tác dễ
Dòng 14: 1 dòng 🔥 — social proof nhẹ (được nhiều khách yêu thích, ưa chuộng...)

3. NGÔN NGỮ:
- Tự nhiên, có cảm xúc, không robot, không liệt kê khô khan.
- KHÔNG dùng các từ trigger FB spam: "mua ngay", "đặt hàng", "sale", "giảm giá", "deal", "khuyến mãi", "rẻ nhất", "freeship", "hoàn tiền", "ship cod", "0đ", "miễn phí".
- KHÔNG phóng đại số liệu, KHÔNG cam kết tuyệt đối ("100% hài lòng", "không lỗi", "tốt nhất").
- KHÔNG thêm hashtag, KHÔNG markdown bold/italic.
- KHÔNG đề cập tên shop hoặc giá tiền (FB ad chuyển đổi tốt khi giấu giá để hook click).

4. OUTPUT FORMAT:
Trả về duy nhất content thuần (không preamble, không giải thích, không quote markdown), bắt đầu chính xác bằng dòng `🍒Xem mẫu : ` và kết thúc ở dòng 🔥. Không thêm xuống dòng thừa ở cuối."#
}

fn user_prompt(p: &GenerateParams) -> String {
    let mut s = format!("Tên sản phẩm: {}\n", p.product_name.trim());
    if !p.shop_name.trim().is_empty() {
        s.push_str(&format!("Shop: {}\n", p.shop_name.trim()));
    }
    if p.sales > 0 {
        s.push_str(&format!("Đã bán: {}\n", p.sales));
    }
    // Giá KHÔNG đưa vào prompt — đã yêu cầu AI không nhắc giá ở Quy tắc 3.
    let _ = p.price;
    s.push_str(
        "\nHãy phân tích ngành hàng từ tên SP rồi viết content FB ads theo cấu trúc đã định.",
    );
    s
}

/// Generate content FB ads từ thông tin sản phẩm. Caller phải đảm bảo
/// `api_key` non-empty (UI đã guard). Lỗi network/4xx/5xx trả lên FE qua
/// `CmdError::Msg` để hiện retry button.
#[tauri::command]
pub async fn ai_generate_fb_content(params: GenerateParams) -> CmdResult<String> {
    let key = params.api_key.trim();
    if key.is_empty() {
        return Err(CmdError::msg("Chưa cấu hình API key OpenAI"));
    }
    let model = params.model.trim();
    if model.is_empty() {
        return Err(CmdError::msg("Chưa chọn model"));
    }
    if params.product_name.trim().is_empty() {
        return Err(CmdError::msg("Tên sản phẩm trống"));
    }

    let body = json!({
        "model": model,
        "temperature": 0.85,
        "max_tokens": 800,
        "messages": [
            { "role": "system", "content": system_prompt() },
            { "role": "user",   "content": user_prompt(&params) },
        ],
    });

    let client = http_client(GENERATE_TIMEOUT_SECS)?;
    let resp = client
        .post(format!("{OPENAI_BASE}/chat/completions"))
        .bearer_auth(key)
        .json(&body)
        .send()
        .await
        .map_err(|e| CmdError::msg(format!("Lỗi kết nối OpenAI: {e}")))?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| CmdError::msg(format!("Lỗi đọc response: {e}")))?;

    if !status.is_success() {
        // Parse OpenAI error envelope nếu có để hiển thị message cụ thể
        // (insufficient_quota, model_not_found, ...).
        if let Ok(parsed) = serde_json::from_str::<ChatResponse>(&text) {
            if let Some(err) = parsed.error {
                let code = err.code.as_deref().unwrap_or("");
                return Err(CmdError::msg(format!(
                    "OpenAI lỗi ({status}{}): {}",
                    if code.is_empty() { String::new() } else { format!(" · {code}") },
                    err.message
                )));
            }
        }
        let preview = &text[..text.len().min(200)];
        return Err(CmdError::msg(format!("HTTP {status}: {preview}")));
    }

    let parsed: ChatResponse = serde_json::from_str(&text).map_err(|e| {
        let preview = &text[..text.len().min(200)];
        CmdError::msg(format!("Phản hồi OpenAI không hợp lệ ({e}): {preview}"))
    })?;

    let content = parsed
        .choices
        .into_iter()
        .next()
        .and_then(|c| c.message.content)
        .ok_or_else(|| CmdError::msg("OpenAI trả response trống"))?;

    Ok(content.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_prompt_omits_shop_when_blank() {
        let p = GenerateParams {
            api_key: "sk-x".into(),
            model: "gpt-4o-mini".into(),
            product_name: "Áo Thun Nữ Babytee".into(),
            shop_name: "".into(),
            price: 99000.0,
            sales: 0,
        };
        let s = user_prompt(&p);
        assert!(s.contains("Áo Thun"));
        assert!(!s.contains("Shop:"));
        assert!(!s.contains("Đã bán:"));
    }

    #[test]
    fn user_prompt_includes_sales_when_positive() {
        let p = GenerateParams {
            api_key: "sk-x".into(),
            model: "gpt-4o-mini".into(),
            product_name: "Máy xay".into(),
            shop_name: "Shop A".into(),
            price: 0.0,
            sales: 1234,
        };
        let s = user_prompt(&p);
        assert!(s.contains("Shop: Shop A"));
        assert!(s.contains("Đã bán: 1234"));
    }

    #[test]
    fn is_chat_model_accepts_gpt_family() {
        assert!(is_chat_model("gpt-4o-mini"));
        assert!(is_chat_model("gpt-4o"));
        assert!(is_chat_model("gpt-4-turbo"));
        assert!(is_chat_model("gpt-3.5-turbo"));
        assert!(is_chat_model("gpt-4.1-mini"));
        assert!(is_chat_model("chatgpt-4o-latest"));
        assert!(is_chat_model("o1"));
        assert!(is_chat_model("o3-mini"));
        assert!(is_chat_model("o4-mini-2025-04-16"));
    }

    #[test]
    fn is_chat_model_rejects_non_chat() {
        assert!(!is_chat_model("text-embedding-3-small"));
        assert!(!is_chat_model("text-embedding-ada-002"));
        assert!(!is_chat_model("whisper-1"));
        assert!(!is_chat_model("tts-1"));
        assert!(!is_chat_model("tts-1-hd"));
        assert!(!is_chat_model("dall-e-3"));
        assert!(!is_chat_model("dall-e-2"));
        assert!(!is_chat_model("babbage-002"));
        assert!(!is_chat_model("davinci-002"));
        assert!(!is_chat_model("text-moderation-latest"));
        assert!(!is_chat_model("gpt-4o-audio-preview"));
        assert!(!is_chat_model("gpt-4o-realtime-preview"));
        assert!(!is_chat_model("gpt-4o-transcribe"));
        assert!(!is_chat_model("omni-moderation-latest"));
    }

    #[test]
    fn user_prompt_never_includes_price() {
        // Giá KHÔNG được vào prompt — đã quy tắc AI không nhắc giá.
        let p = GenerateParams {
            api_key: "sk-x".into(),
            model: "gpt-4o-mini".into(),
            product_name: "X".into(),
            shop_name: "".into(),
            price: 999000.0,
            sales: 0,
        };
        let s = user_prompt(&p);
        assert!(!s.contains("999"));
        assert!(!s.contains("Giá"));
    }
}

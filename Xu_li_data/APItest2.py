import json
import os
import time
from typing import List
from google import genai
from google.genai import types
from pathlib import Path

client = genai.Client()

PROJECT_ROOT = Path(__file__).resolve().parents[1]   # …/SAFEMAP
DATA_DIR     = PROJECT_ROOT / "Data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

prompt_text = """
Bạn là một chuyên gia phân tích thông tin sự cố theo tiêu chuẩn pháp luật Việt Nam. 
Nhiệm vụ của bạn là đọc văn bản mô tả sự cố và xác định:

1. Các lĩnh vực có liên quan (có thể nhiều lĩnh vực).
2. Mức độ khẩn cấp duy nhất (cao nhất) của sự cố.

Các lĩnh vực được xác định theo khung pháp lý Việt Nam (xem định nghĩa bên dưới).
Mỗi lĩnh vực phản ánh loại sự cố chính, còn mức độ khẩn cấp phản ánh mức độ nguy hiểm theo thiệt hại, phạm vi, và khả năng lan rộng.

Kết quả trả về CHỈ là 01 object JSON (không kèm văn bản/Markdown).

YÊU CẦU LỌC TRƯỚC KHI PHÂN LOẠI
1) GIỮ LẠI văn bản chỉ khi đồng thời thỏa mãn:
   (A) Thuộc ≥1 trong 5 lĩnh vực hợp lệ:
       ["Thiên tai & Môi trường","Giao thông & Hạ tầng","Cháy nổ & Sự cố kỹ thuật","An ninh Trật tự Tội phạm","Cộng đồng & Dịch vụ"]
   (B) Có ĐỊA ĐIỂM CỤ THỂ.

2) LOẠI BỎ văn bản nếu không thỏa (A) hoặc (B).

ĐỊNH NGHĨA “ĐỊA ĐIỂM CỤ THỂ”
- Ít nhất ở cấp xã/huyện/tỉnh (ví dụ: “Phường Tây Mỗ, Quận Nam Từ Liêm, Hà Nội”),
  HOẶC có tên đường/QL/Km mốc, HOẶC tên công trình/địa danh xác định (cầu, chợ, trường, bệnh viện…),
  HOẶC tọa độ (lat, lon).
- KHÔNG chấp nhận các chỉ báo mơ hồ như: “Việt Nam”, “miền Bắc”, “nhiều nơi”, “khu vực lân cận”, “trên mạng”
  (nếu không kèm địa danh cụ thể).

ĐỊNH DẠNG TRẢ VỀ (CHỈ JSON)
- Nếu BỊ LOẠI:
{
  "valid": false,
  "discard_reason": ["NO_LOCATION" | "OUT_OF_SCOPE" | "BOTH"],
  "confidence": 0.0,
  "rationale": "Giải thích ngắn tại sao loại."
}

- Nếu GIỮ LẠI (mới phân loại theo yêu cầu gốc và bắt buộc trích xuất địa điểm va url(link bai bao)):
{
  "valid": true,
  "linh_vuc": ["..."], 
  "muc_do_khan_cap": "...", 
  "location": {
    "text": "Phường Tây Mỗ, Quận Nam Từ Liêm, Hà Nội",
    "type": "ADMIN | ROAD | LANDMARK | COORDS",
    "coords": {"lat": 0.0, "lon": 0.0}
  },
  "alt_locations": ["Tây Mỗ","Nam Từ Liêm"],
  "confidence": 0.0-1.0, 
  "rationale": "Lý do xếp lĩnh vực/mức độ + trích xuất địa điểm.",
  "Ngay_thang_nam": ["..."],
  "url": ["..."]
}

QUY TẮC:
- CHỈ 01 object JSON duy nhất trong output (không kèm code fences).
- Nếu có nhiều địa điểm, đưa cái cụ thể nhất vào location.text, còn lại vào alt_locations.
- Không suy đoán địa điểm nếu văn bản không nêu rõ; không mở rộng lĩnh vực ngoài 5 nhóm.
- Nếu chỉ nêu cấp tỉnh/thành mà không cụ thể hơn vẫn chấp nhận, miễn nội dung sự cố đủ rõ ràng; 
  không chấp nhận mức quốc gia/khu vực mơ hồ.

Tài liệu cơ sở lý thuyết: Phân loại thông tin theo
lĩnh vực và mức độ khẩn cấp
I.Phân loại theo lĩnh vực (5 nhóm)
1. Thiên tai & Môi trường
Định nghĩa: Bao gồm các sự kiện tự nhiên hoặc liên quan môi trường gây thiệt hại lớn về người, tài sản
hoặc hệ sinh thái. Theo Luật Phòng, chống thiên tai (2013), “Thiên tai là hiện tượng tự nhiên bất thường
có thể gây thiệt hại về người, tài sản, môi trường…” . Ví dụ điển hình là bão lớn, lũ quét, động đất,
hạn hán, sạt lở đất, ô nhiễm môi trường hay cháy rừng ở quy mô lớn.
Tiêu chí áp dụng: Sự kiện phát sinh từ nguyên nhân tự nhiên hoặc môi trường (trái đất, khí hậu, thủy
văn) và có khả năng gây ảnh hưởng lan rộng. Định tính, thường là cường độ cực đoan (bão cấp mạnh,
lũ vượt mốc lịch sử, động đất mạnh, v.v.), hoặc định lượng như số người thương vong hoặc mức độ
thiệt hại tài sản vượt ngưỡng lớn. Ví dụ, bão số 10 năm 2024 tại Lào Cai gây hậu quả đặc biệt nghiêm
trọng (7 người chết, 3 mất tích, 10 bị thương, hàng nghìn ngôi nhà ngập nước, hạ tầng giao thông, y tế,
giáo dục hư hỏng nặng) . Sự kiện như vậy thuộc nhóm Thiên tai & Môi trường.
Ví dụ thực tế: Các tin bão lớn, lũ dữ, sạt lở đất khiến nhiều huyện bị cô lập; cháy rừng diện rộng; sự cố
tràn dầu, rò rỉ hóa chất gây ô nhiễm sông hồ. (Luật PC thiên tai định danh các loại như bão, mưa lớn, lũ
quét, động đất… là thiên tai ).
2.Giao thông & Hạ tầng
Định nghĩa: Các sự kiện liên quan đến tai nạn giao thông (đường bộ, đường sắt, đường thủy, hàng
không) và sự cố cơ sở hạ tầng (công trình giao thông, điện, cầu đường, viễn thông, năng lượng, v.v.).
Bao gồm cả ùn tắc giao thông nghiêm trọng, sụp đường, sập cầu, mất điện rộng, vỡ ống cấp nước, v.v.
Ví dụ trong thống kê của Bộ Công an, tai nạn giao thông được phân cấp theo mức độ thiệt hại: từ “rất
nghiêm trọng” (như ≥2 người chết) đến “đặc biệt nghiêm trọng” (≥3 người chết) .
Tiêu chí áp dụng: Xác định qua tính chất vật lý (va chạm xe, hỏng đường, đứt cáp điện, v.v.), số người bị
ảnh hưởng và thiệt hại tài sản. Một vụ tai nạn gây nhiều thương vong hoặc hư hại lớn thuộc cấp cao
(theo TT 26/2024, mức “đặc biệt nghiêm trọng” yêu cầu ≥3 người chết hoặc tài sản hư hại ≥1,5 tỷ đồng
). Sự cố hạ tầng lớn (sập cầu, vỡ đập, mất điện diện rộng) cũng nằm trong nhóm này nếu gián đoạn
giao thông hoặc dịch vụ quy mô lớn.
Ví dụ thực tế: Xe khách đâm liên hoàn trên quốc lộ khiến nhiều người tử vong; cầu đường lớn bị lũ
cuốn trôi; nổ đường ống gas tại khu dân cư; mất điện trên diện rộng do sự cố đập thủy điện. (Ví dụ: “vụ
tai nạn giao thông gây hậu quả đặc biệt nghiêm trọng” cần 3 người chết trở lên ).
3.Cháy nổ & Sự cố kỹ thuật
Định nghĩa: Sự kiện cháy, nổ và các sự cố kỹ thuật nghiêm trọng khác không thuộc nguyên nhân thiên
tai. Chẳng hạn: cháy nhà dân và công trình (kể cả cháy chung cư, kho hàng), nổ bình gas, nổ kho xăng
dầu, sự cố vỡ đường ống hóa chất, mất an toàn mạng điện cao thế, tai nạn công nghiệp do máy móc hư
hỏng, v.v.
Tiêu chí áp dụng: Xảy ra do nguồn lửa, khí nổ hoặc lỗi kỹ thuật (chập điện, cháy máy) trong khu vực
cộng đồng, khu công nghiệp; gây nguy cơ lây lan đám cháy hoặc nhiễm độc. Cấp độ được đánh giá dựa
vào mức độ lan rộng, số người thương vong và thiệt hại. Ví dụ, nếu một vụ cháy nhà cao tầng khiến
nhiều người thương vong và nhiều căn hộ bị phá hủy, nó thuộc nhóm này với mức khẩn cấp cao. Ngược
lại, sự cố cháy nhỏ (hỏa hoạn được kiểm soát, không người bị thương) ở quy mô hạn chế có thể chỉ
được xếp ở mức “nhắc nhở”.
Ví dụ thực tế: Cháy ở nhà trọ, trường học, cơ sở sản xuất; nổ kho gas tại khu công nghiệp; sự cố ngập
máy biến áp gây mất điện cục bộ. (Ví dụ thống kê 6 tháng đầu 2025: có 13 vụ nổ, 5 người chết, 27 người
bị thương) đây là kiểu sự kiện “cháy nổ” điển hình của báo cáo PCCC. (Báo cáo giám sát PCCC thường
nêu các con số tương tự để chỉ mức độ nghiêm trọng của cháy nổ.)
4.An ninh Trật tự Tội phạm
Định nghĩa: Các sự kiện liên quan đến an toàn công cộng, an ninh quốc gia, trật tự xã hội và tội phạm.
Theo Nghị định 96/2016/NĐ-CP (điều kiện an ninh trật tự), “An ninh, trật tự là cách viết gọn của cụm từ
an ninh quốc gia, trật tự, an toàn xã hội” . Nhóm này bao gồm việc bảo vệ an ninh quốc gia (khủng
bố, biểu tình bạo loạn, hoạt động gián điệp), duy trì trật tự xã hội (gây rối, bạo loạn, đình công trái pháp
luật) và tội phạm hình sự (giết người, cướp giật, trộm cắp, lừa đảo, ma túy, v.v.).
Tiêu chí áp dụng: Sự kiện có yếu tố vi phạm pháp luật hoặc gây mất trật tự an toàn: số hung thủ, nạn
nhân, tính chất nghiêm trọng của hành vi. Ví dụ, vụ cướp có sử dụng vũ khí hoặc nhiều người bị thương
xếp mức cao; trường hợp đánh nhau hay ẩu đả lớn gây nhiều thương tích cũng bị coi là nghiêm trọng.
Mục tiêu là bảo vệ người dân, trật tự công cộng. Công an các cấp thường căn cứ Bộ luật Hình sự và Luật
Tổ chức Chính quyền địa phương để định mức xử lý hành vi gây mất an ninh, trật tự.
Ví dụ thực tế: Tin tức về bắt giữ băng nhóm tội phạm có vũ trang; tình huống xả súng, đánh nhau
khiến nhiều người bị thương; phong tỏa khu phố có nghi vấn khủng bố; hay tin mới nhất từ Bộ Công
an, Bộ Quốc phòng về mâu thuẫn biên giới. (Dù ví dụ chi tiết rất đa dạng, ND96/2016 có hiệu lực từ
2017 chỉ rõ phạm vi “an ninh, trật tự, an toàn xã hội” được bảo vệ.)
5.Cộng đồng & Dịch vụ
Định nghĩa: Thông tin liên quan đến đời sống cộng đồng và dịch vụ công ích, sức khỏe, giáo dục, xã
hội. Bao gồm các vấn đề như y tế cộng đồng, chính sách xã hội, dịch vụ công (cấp cứu, khẩn cấp y tế, hỗ
trợ thiên tai), giáo dục, chăm sóc dân sinh, hoặc những tin tức tích cực mang tính xây dựng cho cộng
đồng (ví dụ: bình ổn giá cả, thông báo phúc lợi, lễ hội văn hóa, chiến dịch cộng đồng).
Tiêu chí áp dụng: Các sự kiện/kế hoạch có tính cộng đồng, thường không gắn với mối nguy hiểm cấp
tính nhưng quan trọng cho sinh hoạt xã hội. Chúng có thể là thông báo nhắc nhở, khuyến nghị hoặc
dịch vụ cộng đồng (phát vaccine, khuyến cáo sức khỏe, thông tin hỗ trợ, v.v.). Nếu thông tin có lợi cho
cộng đồng, thúc đẩy ý thức, phục vụ dân sinh mà không thuộc các nhóm trên thì xếp vào đây.
Ví dụ thực tế: Tin về chương trình tiêm vaccine phòng bệnh, khuyến cáo vệ sinh mùa dịch của Bộ Y tế;
thông báo cấp cứu miễn phí, khuyến mãi dịch vụ tiện ích cơ sở của chính quyền; hay các chính sách hỗ
trợ người dân sau thiên tai mà các cơ quan ban hành. (Ví dụ: Bộ Y tế khuyến cáo phòng chống dịch
bệnh mới phát hiện; Sở Giao thông thông báo kế hoạch sửa chữa cầu, đường sắp diễn ra.)
II.Phân loại theo mức độ khẩn cấp (4 mức)
Việc đánh giá mức độ khẩn cấp (cấp độ ưu tiên) dựa trên tiêu chí rủi ro của sự kiện như mức độ nguy
hiểm, phạm vi ảnh hưởng, số người bị ảnh hưởng, thiệt hại tài sản, khả năng lan rộng… (Luật Phòng
chống thiên tai cũng nêu các yếu tố này khi phân cấp rủi ro thiên tai: “cường độ…, phạm vi…, khả năng
gây thiệt hại đến tính mạng, tài sản, công trình và môi trường” ). Từ đó, ta chia thành 4 mức:
1.Cảnh báo nguy hiểm: Mức cao nhất. Áp dụng khi sự kiện có hậu quả nghiêm trọng, uy hiếp lớn
đến tính mạng và tài sản. Ví dụ: nhiều người thương vong (đặc biệt ≥3 người chết), thiệt hại quy
mô lớn hoặc nguy cơ lan rộng nhanh chóng. Thường là các cấp độ rủi ro cực cao, như bão cấp 12
đổ bộ, lũ lịch sử, hoả hoạn lớn, tấn công khủng bố,… Trong nhóm này, cần ưu tiên khẩn cấp
tuyệt đối: huy động mọi nguồn lực (y tế, cứu hộ, an ninh). Ví dụ thực tế: Bão số 10 năm 2024 ở
Lào Cai được xem là đặc biệt nghiêm trọng (7 người chết, 3 mất tích) ; hay một vụ tai nạn giao
thông “gây hậu quả đặc biệt nghiêm trọng” (TT26/2024: ≥3 người chết hoặc thiệt hại tài sản
≥1,5 tỷ đồng) cũng thuộc cảnh báo nguy hiểm .
2.Cảnh báo trung bình: Mức trung bình, dành cho các sự kiện có mức độ nguy hiểm vừa phải. Ví
dụ có người bị thương nặng hoặc vài người tử vong nhưng quy mô hẹp hơn. Thiệt hại tài sản ở
mức vừa phải. Các tình huống này vẫn cần ngăn ngừa và cảnh báo nhưng không ở tình trạng
cực kỳ khẩn cấp. Ví dụ: vụ tai nạn giao thông làm chết 1–2 người, hay một đám cháy công nghiệp
nhỏ gây thương vong hạn chế. Trong TT26/2024, tai nạn “rất nghiêm trọng” với 2 người chết,
hoặc “nghiêm trọng” với 1 người chết, sẽ được xem là mức cảnh báo trung bình . Mức cảnh
báo trung bình yêu cầu các cơ quan chức năng chủ động theo dõi và xử lý, sơ tán/thông báo cho
dân trú ẩn nếu cần.
3.Nhắc nhở: Mức thấp, dành cho các sự kiện ít nghiêm trọng. Có thể xảy ra thiệt hại nhỏ (chỉ bị
thương nhẹ, không tử vong, hoặc tài sản hư hại ít), phạm vi hạn chế và khả năng lan rộng thấp.
Đây thường là thông báo phòng ngừa, hướng dẫn người dân cẩn trọng. Ví dụ: Tai nạn giao
thông nhẹ (chỉ trầy xước nhỏ), cháy nhỏ đã được dập tắt, sự cố điện không gây thương tích. Ở
mức này, thông tin chủ yếu mang tính cảnh báo sớm để tránh tái diễn, không đòi hỏi khẩn cấp
huy động lực lượng lớn. (Theo TT26/2024, tai nạn “ít nghiêm trọng” – chẳng hạn 1 người thương
tích dưới 61% hoặc tài sản thiệt hại từ 10–100 triệu – tương ứng nhắc nhở.)
4.Tích cực: Mức thông tin bình thường hoặc mang tính “tích cực, xây dựng” cho cộng đồng. Không
phải là cảnh báo về mối nguy hiểm. Nhãn “tích cực” dùng cho các tin có nội dung tích cực,
khuyến khích, thông báo chung về dịch vụ hoặc phúc lợi cộng đồng (ví dụ: khuyến cáo tập luyện
nâng cao sức khỏe, sự kiện cộng đồng, tình hình phát triển tích cực). Loại này phản ánh mức độ
thấp nhất về độ khẩn cấp. Ví dụ: Khuyến cáo thường xuyên rửa tay của Bộ Y tế, hay thông báo
lịch cắt điện theo kế hoạch, thông tin hỗ trợ người dân… – mọi chuyện ở trạng thái an toàn,
không có sự cố. Mục tiêu ở mức này là duy trì nhận thức cộng đồng, nâng cao phòng ngừa chứ
không phải ứng phó khẩn cấp.
Các mức độ trên được áp dụng linh hoạt tùy theo dấu hiệu nhận biết trong sự kiện cụ thể. Ví dụ, nếu một
vùng chịu thiên tai cường độ cao (bão mạnh kết hợp lũ quét) gây nhiều người thương vong và hư hỏng
hàng loạt, sẽ xếp Cảnh báo nguy hiểm. Nếu cùng sự kiện nhưng tác động giới hạn (thiệt hại nhẹ, không
người chết), có thể xếp Nhắc nhở hoặc Tích cực kèm khuyến cáo. Ta luôn cân nhắc số người ảnh hưởng,
thiệt hại về người-tài sản, nguy cơ mở rộng (ví dụ sạt lở tiếp theo, hỏa hoạn lan rộng…) để phân chia.
Căn cứ pháp lý (như Điều 18 Luật PC thiên tai ) cho thấy ba tiêu chí chính: cường độ nguy hiểm,
phạm vi ảnh hưởng, thiệt hại đến người-tài sản.
Trong đó linh_vuc là một trong 5 nhóm nêu trên, muc_do_khan_cap là một trong 4 mức khẩn cấp
tương ứng và noi_dung chứa thông tin sự kiện.
Các nguồn tham khảo: Luật Phòng, chống thiên tai 2013 ; Thông tư 26/2024/TT-BCA về phân
loại tai nạn giao thông ; Nghị định 96/2016/NĐ-CP (điều kiện an ninh trật tự) ; các bản tin chính
phủ và báo chí chính thống cập nhật các sự kiện thiên tai, tai nạn, an ninh, v.v., để làm ví dụ.
Luật Phòng chống thiên tai mới nhất, số 33/2013/QH13
https://luatvietnam.vn/tai-nguyen/luat-phong-chong-thien-tai-2013-79379-d1.html
Bão số 11 hết sức nguy hiểm, phải nhận thức rõ tính chất đặc biệt phức tạp của tình hình để kịp thời
ứng phó
https://xaydungchinhsach.chinhphu.vn/bao-so-11-het-suc-nguy-hiem-phai-nhan-thuc-ro-tinh-chat-dac-biet-phuc-tap-cuatinh-hinh-de-kip-thoi-ung-pho-119251004134206226.htm
Quy định mới về phân loại tai nạn giao thông
https://xaydungchinhsach.chinhphu.vn/quy-dinh-moi-ve-phan-loai-tai-nan-giao-thong-119240814154509006.htm
Nghị định 96/2016/NĐ-CP hoạt động kinh doanh ngành nghề đầu tư kinh doanh điều kiện an ninh
trật tự mới nhất
https://thuvienphapluat.vn/van-ban/Thuong-mai/Nghi-dinh-96-2016-ND-CP-hoat-dong-kinh-doanh-nganh-nghe-dau-tu-kinhdoanh-dieu-kien-an-ninh-trat-tu-315469.aspx
Ví dụ 1 (GIỮ LẠI):
Văn bản: Xe container va chạm với 3 xe máy trên QL1, 4 người thương nặng, giao thông tê liệt
Output:
{"valid":true,"linh_vuc":["Giao thông & Hạ tầng"],"muc_do_khan_cap":"Cảnh báo nguy hiểm","location":{"text":"Quốc lộ 1 (QL1)","type":"ROAD"},"alt_locations":[],"confidence":0.95,"rationale":"Có địa điểm cụ thể (QL1). Nhiều người bị thương nặng và tắc nghẽn nghiêm trọng nên xếp mức cao."}

Ví dụ 2 (LOẠI BỎ – thiếu địa điểm):
Văn bản: Xe máy bị lật nhẹ do ướt đường, không ai bị thương nặng
Output:
{"valid":false,"discard_reason":["NO_LOCATION"],"confidence":0.8,"rationale":"Thuộc lĩnh vực Giao thông nhưng không nêu địa điểm cụ thể nên bị loại theo tiêu chí bắt buộc địa điểm."}

Ví dụ 3 (GIỮ LẠI – thông tin tích cực có địa điểm):
Văn bản: Trạm Y tế phường Tây Mỗ tổ chức tiêm vaccine cúm miễn phí ngày 05/10
Output:
{"valid":true,"linh_vuc":["Cộng đồng & Dịch vụ"],"muc_do_khan_cap":"Tích cực","location":{"text":"Phường Tây Mỗ","type":"ADMIN"},"alt_locations":[],"confidence":0.9,"rationale":"Thông tin dịch vụ cộng đồng, có địa điểm cụ thể (phường Tây Mỗ). Không phải cảnh báo nguy hiểm nên xếp 'Tích cực'."}

Ví dụ 4 (LOẠI BỎ – ngoài phạm vi lĩnh vực và thiếu địa điểm):
Văn bản: Giá cổ phiếu công nghệ tăng mạnh trong phiên sáng nay
Output:
{"valid":false,"discard_reason":["OUT_OF_SCOPE","NO_LOCATION"],"confidence":0.9,"rationale":"Chủ đề kinh tế/chứng khoán không thuộc 5 lĩnh vực, đồng thời không có địa điểm cụ thể."}
"""

#input
INPUT_JSON = DATA_DIR / "safemap_data.json"

# ====== Hàm tiện ích ======
def chunk_list(items: List[str], n: int) -> List[List[str]]:
    """Chia list thành các khúc kích thước n."""
    return [items[i:i+n] for i in range(0, len(items), n)]

def clean_and_parse_json(raw: str):
    """Bỏ code fences và parse JSON; fallback về list/dict rỗng nếu lỗi."""
    s = raw.strip()
    if s.startswith("```"):
        # Xóa ```json ... ```
        s = s.strip("`")
        s = s.replace("json\n", "", 1).rstrip("`")
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        try:
            return [json.loads(line) for line in s.splitlines() if line.strip().startswith("{")]
        except Exception:
            return None

def save_jsonl(path: str, obj):
    """Ghi 1 object mỗi dòng (append, không ghi đè)."""
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

# ====== Đọc dữ liệu ======
def build_incident_text(item: dict) -> str:
    parts = []
    title = item.get("title")
    summary = item.get("summary")
    date = item.get("date")
    source = item.get("source")
    url = item.get("url")
    if title:
        parts.append(title.strip())
    if summary:
        parts.append(summary.strip())
    if date:
        parts.append(f"Ngày: {date}")
    if source:
        parts.append(f"Nguồn: {source}")
    if url:
        parts.append(f"Url: {url}")
    return " ".join(parts)

with open(INPUT_JSON, "r", encoding="utf-8") as f:
    data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("File JSON phải là một mảng các object.")
    incidents = [build_incident_text(x) for x in data if isinstance(x, dict)]

# ====== In chuyên nghiệp (tùy chọn dùng rich nếu có) ======
import sys
import textwrap
from collections import Counter

try:
    from rich.console import Console
    from rich.table import Table
    RICH_AVAILABLE = True
    console = Console()
except Exception:
    RICH_AVAILABLE = False

def _short(s: str, width: int = 40) -> str:
    s = (s or "").strip()
    if not s:
        return "-"
    return textwrap.shorten(s, width=width, placeholder="…")

def print_batch_table(batch_id: int, total_batches: int, rows: list):
    """
    rows: list[dict] với các khóa:
      index, status, linh_vuc, muc_do, location, discard, confidence, url
    """
    if RICH_AVAILABLE:
        table = Table(title=f"Batch {batch_id}/{total_batches}", show_lines=False, expand=False)
        table.add_column("Index", justify="right", style="bold")
        table.add_column("Trạng thái")
        table.add_column("Lĩnh vực")
        table.add_column("Mức độ")
        table.add_column("Địa điểm")
        table.add_column("Lý do loại")
        table.add_column("Conf.")
        table.add_column("Url")

        for r in rows:
            conf_str = f"{r['confidence']:.2f}" if isinstance(r["confidence"], (int, float)) else "-"
            table.add_row(
                str(r["index"]),
                r["status"],
                _short(r["linh_vuc"], 34),
                _short(r["muc_do"], 20),
                _short(r["location"], 36),
                _short(r["discard"], 28),
                conf_str,
                _short(r["url"], 60),
            )
        console.print(table)
    else:
        header = f"== Batch {batch_id}/{total_batches} =="
        print(header)
        print("-" * len(header))
        print(f"{'Index':>5} | {'Trạng thái':10} | {'Lĩnh vực':34} | {'Mức độ':20} | {'Địa điểm':36} | {'Lý do loại':28} | {'Conf.':6} | {'Url':60}")
        print("-" * 210)
        for r in rows:
            conf_str = f"{r['confidence']:.2f}" if isinstance(r["confidence"], (int, float)) else "-"
            print(
                f"{r['index']:>5} | "
                f"{_short(r['status'],10):10} | "
                f"{_short(r['linh_vuc'],34):34} | "
                f"{_short(r['muc_do'],20):20} | "
                f"{_short(r['location'],36):36} | "
                f"{_short(r['discard'],28):28} | "
                f"{conf_str:>6} | "
                f"{_short(r['url'],60):60}"
            )
        print()

def summarize_and_print(rows: list):
    """In tóm tắt số lượng giữ/lọc."""
    c = Counter(r["status"] for r in rows)
    kept = c.get("OK", 0)
    dropped = c.get("DROP", 0)
    print(f"Tóm tắt: giữ {kept} | loại {dropped}\n")

# ====== Xử lý theo batch 30 sự cố ======
output_jsonl = DATA_DIR / "ket_qua.jsonl"
output_jsonl.parent.mkdir(parents=True, exist_ok=True)

batches = chunk_list(incidents, 30)
global_index_start = 1

for batch_id, batch in enumerate(batches, start=1):
    batch_indices = list(range(global_index_start, global_index_start + len(batch)))
    seed_list = [{"index": idx, "noi_dung": txt} for idx, txt in zip(batch_indices, batch)]

    # LƯU Ý: sửa "url: [...]" -> "url": [...] trong schema mẫu để model trả đúng key
    batch_prompt = f"""{prompt_text}

Hãy phân loại TOÀN BỘ danh sách văn bản sau và TRẢ VỀ DUY NHẤT MỘT MẢNG JSON.
Mỗi phần tử có dạng:
{{
  "index": <int>,
  "noi_dung": <string>,
  "valid": <bool>,
  "linh_vuc": [...],
  "muc_do_khan_cap": "...",
  "location": {{"text": "...", "type": "ADMIN | ROAD | LANDMARK | COORDS", "coords": {{"lat": 0.0, "lon": 0.0}}}},
  "alt_locations": [...],
  "url": [...],
  "confidence": <0..1>,
  "rationale": "...",
  "discard_reason": ["..."]
}}

Danh sách sự cố (mảng JSON):
{json.dumps(seed_list, ensure_ascii=False, indent=2)}
"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=batch_prompt,
            config=types.GenerateContentConfig(
                # response_mime_type="application/json",
                thinking_config=types.ThinkingConfig(thinking_budget=0),
            ),
        )

        raw_text = response.text
        parsed = clean_and_parse_json(raw_text)

        if not isinstance(parsed, list):
            debug_path = DATA_DIR / f"debug_batch_{batch_id}.txt" 
            with debug_path.open("w", encoding="utf-8") as dbg:
                dbg.write(raw_text)
            print(f"[CẢNH BÁO] Không parse được JSON cho batch {batch_id}. Đã lưu thô: {debug_path}")
            global_index_start += len(batch)
            continue

        by_index = {obj.get("index"): obj for obj in parsed if isinstance(obj, dict)}

        printable_rows = []
        for idx, txt in zip(batch_indices, batch):
            obj = by_index.get(
                idx,
                {
                    "index": idx,
                    "noi_dung": txt,
                    "valid": False,
                    "discard_reason": ["MODEL_MISSED"],
                    "confidence": 0.0,
                    "rationale": "Model không trả về mục này."
                }
            )
            obj.setdefault("noi_dung", txt)

            # Ghi JSONL
            save_jsonl(output_jsonl, obj)

            # Chuẩn hóa URL để in
            url_val = obj.get("url", "-")
            if isinstance(url_val, list):
                url_str = "; ".join(str(u) for u in url_val)
            elif isinstance(url_val, str):
                url_str = url_val
            else:
                url_str = "-"

            # Hàng in
            valid = obj.get("valid") is True
            status = "OK" if valid else "DROP"
            linh_vuc = ", ".join(obj.get("linh_vuc", [])) if valid else "-"
            muc_do = obj.get("muc_do_khan_cap") if valid else "-"
            loc_text = (obj.get("location") or {}).get("text") if valid else "-"
            discard = ", ".join(obj.get("discard_reason", [])) if not valid else ""
            conf = obj.get("confidence", "-")

            printable_rows.append({
                "index": idx,
                "status": status,
                "linh_vuc": linh_vuc or "-",
                "muc_do": muc_do or "-",
                "location": loc_text or "-",
                "discard": discard or "",
                "confidence": conf,
                "url": url_str or "-",
            })

        print_batch_table(batch_id, len(batches), printable_rows)
        summarize_and_print(printable_rows)

        global_index_start += len(batch)

    except Exception as e:
        print(f"[LỖI] Batch {batch_id}: {e}")

print(f"Hoàn tất. File kết quả (JSON Lines): {output_jsonl}")

import pytest
pytest.importorskip("streamlit")
from modules.payment_reconciliation import _is_time_remark


@pytest.mark.parametrize("text", [
    "LC00216012 專員回報減時--專員回報減時2人2小時，待退服務費$2400",
    "因專員遲到現場減時1人1小時，刷退$600",
    "異動補9/30 客訴2人8分鐘(B)",
    "專員回報加時2人1小時，待收服務費$1400",
    "加時 1.5 小時",
    "減時30分",
    "加時2個小時",
])
def test_time_remarks_marked(text):
    assert _is_time_remark(text)


@pytest.mark.parametrize("text", [
    "異動服務退款--異動至平日服務退款 $600，09/27已退",
    "服務前一天異動，收異動費$800，開立發票FP56436112",
    "異動地毯清潔服務 2160*1.05=2268",
    "服務當天異動，扣除50%異動費退款$ 4200",
    "服務前2天取消，收異動費$1080",
    "週末加價費用，待收$600",
    "部分退款",
    "",
])
def test_money_remarks_not_marked(text):
    assert not _is_time_remark(text)

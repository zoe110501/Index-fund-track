from datetime import datetime, timedelta, timezone

from financial_hot_terminal.core import (
    REVIEW_APPROVED,
    REVIEW_CANDIDATE,
    REVIEW_SELECTED,
    SOURCE_OFFICIAL,
    SOURCE_SOCIAL,
    RawItem,
    build_hotspot_from_items,
    compliance_violations,
    calculate_hot_score,
    is_selected_candidate,
)


SHANGHAI = timezone(timedelta(hours=8))


def make_item(
    item_id: str,
    *,
    source_type: str = SOURCE_OFFICIAL,
    title: str = "机器人产业链政策更新",
    published_at: datetime | None = None,
) -> RawItem:
    return RawItem(
        id=item_id,
        source_id=f"{source_type}-source",
        source_name="示例源",
        source_type=source_type,
        title=title,
        url=f"https://example.com/{item_id}",
        content="政策与产业链事件影响机器人、人工智能和中证A500相关产品。",
        published_at=published_at or datetime(2026, 5, 9, 10, 0, tzinfo=SHANGHAI),
        market="A股",
    )


def test_score_formula_uses_fixed_weights():
    score = calculate_hot_score(
        market_impact=80,
        product_relevance=60,
        verification_confidence=50,
        freshness=40,
        social_heat=20,
    )

    assert score == 62.0


def test_social_only_item_stays_candidate_until_verified_or_approved():
    hotspot = build_hotspot_from_items(
        [make_item("social-1", source_type=SOURCE_SOCIAL)],
        now=datetime(2026, 5, 9, 11, 0, tzinfo=SHANGHAI),
    )

    assert hotspot.review_status == REVIEW_CANDIDATE
    assert not is_selected_candidate(hotspot)
    assert hotspot.verification_status == "待二次确认"


def test_social_item_with_official_confirmation_can_enter_selection():
    hotspot = build_hotspot_from_items(
        [
            make_item("social-1", source_type=SOURCE_SOCIAL),
            make_item("official-1", source_type=SOURCE_OFFICIAL),
        ],
        now=datetime(2026, 5, 9, 11, 0, tzinfo=SHANGHAI),
    )

    assert hotspot.review_status == REVIEW_SELECTED
    assert is_selected_candidate(hotspot)
    assert hotspot.verification_status == "已交叉验证"


def test_manual_approval_allows_social_item_into_selection():
    hotspot = build_hotspot_from_items(
        [make_item("social-1", source_type=SOURCE_SOCIAL)],
        now=datetime(2026, 5, 9, 11, 0, tzinfo=SHANGHAI),
        manual_review_status=REVIEW_APPROVED,
    )

    assert is_selected_candidate(hotspot)
    assert hotspot.review_status == REVIEW_APPROVED


def test_compliance_violations_flag_investment_advice_language():
    violations = compliance_violations("建议买入该ETF，目标收益30%，卖出其他产品。")

    assert "买入" in violations
    assert "卖出" in violations
    assert "目标收益" in violations

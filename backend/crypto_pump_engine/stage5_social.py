from __future__ import annotations

from .models import IntakeResult, SocialResult


def run_stage(intake: IntakeResult) -> SocialResult:
    ### LIVE API ###
    link_count = len(intake.socials)
    website_count = len(intake.websites)
    boost_count = int(intake.boost_count or 0)
    paid_order_count = int(intake.paid_order_count or 0)

    ### HEURISTIC ###
    social_score = min(
        100.0,
        (link_count * 18.0)
        + (website_count * 12.0)
        + (boost_count * 5.0)
        + (paid_order_count * 4.0),
    )

    if link_count or website_count:
        source_mode = "live_lite"
        risks = []
    else:
        ### MOCK (replace later) ###
        source_mode = "fallback_demo"
        social_score = max(social_score, 12.0)
        risks = ["social_fallback_mode"]

    if boost_count >= 8:
        risks.append("aggressive_boosting")

    return SocialResult(
        social_score=round(social_score, 2),
        source_mode=source_mode,
        link_count=link_count,
        website_count=website_count,
        boost_count=boost_count,
        paid_order_count=paid_order_count,
        risks=sorted(set(risks)),
    )

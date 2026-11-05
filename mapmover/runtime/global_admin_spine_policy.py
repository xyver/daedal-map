"""Admission rules for administrative results from the Global fallback."""

def admitted_admin_match(baseline_loc_id: str, admin_level: int) -> bool:
    """Exclude confirmed nonadministrative Global levels from admin results."""
    # All 76 Global Canada ADM2 shapes are economic regions. Even seven
    # translated IDs are not evidence of census-division containment.
    return not (baseline_loc_id.startswith("CAN-") and admin_level == 2)

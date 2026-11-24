"""Admission rules for administrative results from the Global fallback."""

def admitted_admin_match(baseline_loc_id: str, admin_level: int) -> bool:
    """Exclude confirmed nonadministrative Global levels from admin results."""
    # All 76 Global Canada ADM2 shapes are economic regions. Even seven
    # translated IDs are not evidence of census-division containment.
    return not (baseline_loc_id.startswith("CAN-") and admin_level == 2)


# Global results retain their source IDs; country scopes use country IDs.
GLOBAL_ADMIN2_SYSTEMS = {
    "geometry_source": "GeoBoundaries Admin 2",
    "country_source": "country-pack Admin 2",
    "relationship": "The two Admin 2 systems can define different areas. A shared Admin 2 label does not make their boundaries interchangeable.",
}

from agent_surface_shared import (
    geography_workflow_section,
    render_app_llms_txt,
    render_site_llms_full,
    render_site_llms_txt,
)
from pack_registry_shared import tool_family_catalog_entry


def test_llm_surfaces_cover_every_current_geography_tool() -> None:
    tool_names = [
        str(item.get("name") or "")
        for item in tool_family_catalog_entry("geography").get("tools") or []
    ]
    surfaces = (render_app_llms_txt(), render_site_llms_txt(), render_site_llms_full())

    assert len(tool_names) == 17
    for surface in surfaces:
        for tool_name in tool_names:
            assert f"`{tool_name}`" in surface
        assert "All packs share a loc_id" not in surface
        assert "four user-facing modes" not in surface
        assert "include_references=true" in surface
        assert "country_scope=<ISO3>" in surface
        assert "Admin1 owner" in surface
        assert "get_boundary" not in surface
        assert "loc_id_hierarchy" not in surface
        assert "loc_id_references" not in surface
    for surface in surfaces[1:]:
        assert "reusable geometry" in surface
        assert "published crosswalks" in surface


def test_geography_workflow_is_question_first_and_bounded() -> None:
    workflow = geography_workflow_section()

    assert "Point to loc_id" in workflow
    assert "What a loc_id is connected to" in workflow
    assert "Shape lookup" in workflow
    assert "bounded batch" in workflow
    assert "Advanced builder foundation, not the normal entry path" in workflow
    assert "Durable uploads, saved projects, and custom downloadable artifacts" in workflow

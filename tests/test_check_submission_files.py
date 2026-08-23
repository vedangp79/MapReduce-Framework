"""Verify required files are present."""

import os
import bs4
import markdown


def test_check_submission_files():
    """Check for files and directories required by the spec."""
    assert os.path.exists("bin")
    assert os.path.exists("bin/mapreduce")
    assert os.path.exists("pyproject.toml")
    assert os.path.exists("mapreduce")
    assert os.path.exists("mapreduce/manager/__init__.py")
    assert os.path.exists("mapreduce/manager/__main__.py")
    assert os.path.exists("mapreduce/worker/__init__.py")
    assert os.path.exists("mapreduce/worker/__main__.py")
    assert os.path.exists("mapreduce/__init__.py")
    assert os.path.exists("mapreduce/submit.py")
    assert (
        os.path.exists("mapreduce/utils.py") or
        os.path.exists("mapreduce/utils")
    )


def validate_multiple_choice(ele, qid):
    """Validate multiple choice response and return answer."""
    assert ele.name == "ul"

    selection = ""
    for list_item in ele.find_all("li"):
        if list_item.text.lower().startswith("[x]"):
            assert not selection, f"Q{qid}: Expected one selection '[x]'"
            selection = list_item.text.replace("[x]", "").strip()
    assert selection

    return selection


def validate_muliple_selection(ele, qid):
    """Validate multiple selection response and return answer."""
    assert ele.name == "ul"

    selections = {}
    for i, list_item in enumerate(ele.find_all("li"), start=1):
        sub_qid = f"{qid}.{i}"
        if list_item.text.lower().startswith("[x]"):
            selections[sub_qid] = "Yes"
        else:
            selections[sub_qid] = "No"

    return selections

import subprocess
from os.path import abspath, dirname

from utils import all, good, reset

project_path = dirname(dirname(abspath(__file__)))
meltano_path = "/Users/pnadolny/Documents/Git/GitHub/meltano/meltano/"

core = f"""MELTANO_PROJECT_ROOT={project_path}/meltano_project/telemetry_testing MELTANO_SNOWPLOW_COLLECTOR_ENDPOINTS='["http://localhost:9090"]'"""
usage_stats = "MELTANO_SEND_ANONYMOUS_USAGE_STATS=True MELTANO_DISABLE_TRACKING=False"


def assert_shared_context_uuid(data, event_count):
    context_uuids = []
    for event in data:
        contexts = event.get("event").get("contexts").get("data")
        if not contexts:
            raise Exception("Missing context")
        for context in contexts:
            schema = context.get("schema")
            if schema.startswith("iglu:com.meltano/project_context/"):
                context_uuid = context.get("data").get("context_uuid")
                context_uuids.append(context_uuid)
    assert len(context_uuids) == event_count, data
    assert len(set(context_uuids)) == 1, data


def test_invoke_good():
    reset()
    subprocess.run(
        f"{usage_stats} {core} poetry run meltano --log-level=debug invoke tap-csv" "",
        shell=True,
        executable="/bin/bash",
        cwd=meltano_path,
        check=True,
    )
    data = all()
    assert data["total"] == 3, data
    assert data["good"] == 3, data
    assert data["bad"] == 0, data
    # shared context_uuid
    data = good()
    assert_shared_context_uuid(data, 3)


def test_invoke_not_found():
    reset()
    subprocess.run(
        f"{usage_stats} {core} poetry run meltano invoke tap-foo" "",
        shell=True,
        executable="/bin/bash",
        cwd=meltano_path,
        # check=True
    )
    data = all()
    assert data["total"] == 2, data
    assert data["good"] == 2, data
    assert data["bad"] == 0, data


def test_invoke_bad_config():
    reset()
    subprocess.run(
        f"{usage_stats} {core} poetry run meltano invoke tap-csv-bad" "",
        shell=True,
        executable="/bin/bash",
        cwd=meltano_path,
        # check=True
    )
    data = all()
    assert data["total"] == 2, data
    assert data["good"] == 2, data
    assert data["bad"] == 0, data
    # TODO: assert 1 is failed


def test_run_good():
    reset()
    subprocess.run(
        f"{usage_stats} {core} poetry run meltano run tap-csv target-jsonl" "",
        shell=True,
        executable="/bin/bash",
        cwd=meltano_path,
        check=True,
    )
    data = all()
    assert data["total"] == 5, data
    assert data["good"] == 5, data
    assert data["bad"] == 0, data
    # shared context_uuid
    data = good()
    assert_shared_context_uuid(data, 5)


def remove_cleanup(name):
    subprocess.run(
        f"{usage_stats} {core} poetry run meltano remove {name}" "",
        shell=True,
        executable="/bin/bash",
        cwd=meltano_path,
        # check=True
    )


def test_add_good():
    reset()
    try:
        subprocess.run(
            f"{usage_stats} {core} poetry run meltano add extractor tap-carbon-intensity"
            "",
            shell=True,
            executable="/bin/bash",
            cwd=meltano_path,
            check=True,
        )
    finally:
        remove_cleanup("extractor tap-carbon-intensity")
    data = all()
    assert data["total"] == 3, data
    assert data["good"] == 3, data
    assert data["bad"] == 0, data
    # TODO: assert 1 struct add and a start/completed add + extractors


def test_add_not_found():
    reset()
    subprocess.run(
        f"{usage_stats} {core} poetry run meltano add extractor tap-foo-bar" "",
        shell=True,
        executable="/bin/bash",
        cwd=meltano_path,
        # check=True
    )
    data = all()
    assert data["total"] == 2, data
    assert data["good"] == 2, data
    assert data["bad"] == 0, data
    # TODO: assert start/aborted, no struct


def test_add_failed_install():
    reset()
    try:
        # TODO: find a new way to do this
        subprocess.run(
            f"{usage_stats} {core} poetry run meltano add loader target-postgres" "",
            shell=True,
            executable="/bin/bash",
            cwd=meltano_path,
            # check=True
        )
    finally:
        remove_cleanup("loader target-postgres")
    data = all()
    assert data["total"] == 3, data
    assert data["good"] == 3, data
    assert data["bad"] == 0, data
    # TODO: assert start/failed, struct


def test_install_good():
    reset()
    # remove it if it exists
    subprocess.run(
        f"rm -r {project_path}/meltano_project/telemetry_testing/.meltano/extractors/tap-csv",
        shell=True,
    )
    # TODO: find a new way to do this
    subprocess.run(
        f"{usage_stats} {core} poetry run meltano install extractor tap-csv" "",
        shell=True,
        executable="/bin/bash",
        cwd=meltano_path,
        check=True,
    )
    data = all()
    assert data["total"] == 3, data
    assert data["good"] == 3, data
    assert data["bad"] == 0, data
    # TODO: assert start/completed, struct


def test_install_not_exists():
    reset()
    subprocess.run(
        f"{usage_stats} {core} poetry run meltano install extractor tap-foo-bar-baz" "",
        shell=True,
        executable="/bin/bash",
        cwd=meltano_path,
        # check=True
    )
    data = all()
    assert data["total"] == 3, data
    assert data["good"] == 3, data
    assert data["bad"] == 0, data
    # TODO: assert start/completed, struct


def test_install_invalid():
    reset()
    subprocess.run(
        f"{usage_stats} {core} poetry run meltano install extractor tap-something-invalid"
        "",
        shell=True,
        executable="/bin/bash",
        cwd=meltano_path,
        # check=True
    )
    data = all()
    assert data["total"] == 2, data
    assert data["good"] == 2, data
    assert data["bad"] == 0, data
    # TODO: assert start/completed, struct


def test_opt_out():
    reset()
    try:
        subprocess.run(
            f"rm -r {project_path}/meltano_project/telemetry_testing/.meltano/analytics.json",
            shell=True,
        )
        test_invoke_good()
        reset()
        subprocess.run(
            f"{core} poetry run meltano config meltano set send_anonymous_usage_stats false"
            "",
            shell=True,
            executable="/bin/bash",
            cwd=meltano_path,
            # check=True
        )
        data = all()
        assert data["total"] == 0, data
        assert data["good"] == 0, data
        assert data["bad"] == 0, data
        reset()
        subprocess.run(
            f"{core} poetry run meltano invoke tap-csv" "",
            shell=True,
            executable="/bin/bash",
            cwd=meltano_path,
            # check=True
        )
        data = all()
        assert data["total"] == 1, data
        assert data["good"] == 1, data
        assert data["bad"] == 0, data
        data = good()
        for event in data:
            unstruct_event = event.get("event").get("unstruct_event").get("data")
            assert unstruct_event.get("schema").startswith(
                "iglu:com.meltano/telemetry_state_change_event/jsonschema"
            )
            assert (
                unstruct_event.get("data").get("setting_name")
                == "send_anonymous_usage_stats"
            )
            assert unstruct_event.get("data").get("changed_from") == True
            assert unstruct_event.get("data").get("changed_to") == False
    finally:
        subprocess.run(
            f"{core} poetry run meltano config meltano set send_anonymous_usage_stats true"
            "",
            shell=True,
            executable="/bin/bash",
            cwd=meltano_path,
            # check=True
        )
        subprocess.run(
            f"rm -r {project_path}/meltano_project/telemetry_testing/.meltano/analytics.json",
            shell=True,
        )


if __name__ == "__main__":
    test_invoke_good()
    test_invoke_not_found()
    test_invoke_bad_config()
    test_run_good()
    test_add_good()
    test_add_not_found()
    test_add_failed_install()
    test_install_good()
    test_install_not_exists()
    test_opt_out()
    # TODO: fix this
    # test_install_invalid()

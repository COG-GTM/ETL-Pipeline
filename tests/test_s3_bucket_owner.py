import subprocess
import sys
from pathlib import Path
from unittest import mock

import boto3
import botocore.exceptions
import pandas as pd
import pytest
from botocore.stub import Stubber

import main
from src import load_data_to_s3

REPO = Path(__file__).resolve().parents[1]
OWNER = "111122223333"
DF = pd.DataFrame({"vin": ["V1"], "buyer_name": ["Alice"]})


@pytest.fixture
def stubbed_s3(monkeypatch):
    client = boto3.client("s3", region_name="us-west-2",
                          aws_access_key_id="x", aws_secret_access_key="y")
    monkeypatch.setattr(load_data_to_s3, "connect_to_s3", lambda *a, **k: client)
    with Stubber(client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


def test_put_object_sends_expected_bucket_owner(stubbed_s3):
    stubbed_s3.add_response("put_object", {}, {
        "Bucket": "my-bucket", "Key": "k.csv",
        "Body": "vin,buyer_name\nV1,Alice\n", "ExpectedBucketOwner": OWNER,
    })
    load_data_to_s3.df_to_s3(DF, "k.csv", "my-bucket", "x", "y", expected_bucket_owner=OWNER)


def test_bucket_owned_by_other_account_raises(stubbed_s3, capsys):
    stubbed_s3.add_client_error("put_object", service_error_code="AccessDenied",
                                http_status_code=403)
    with pytest.raises(botocore.exceptions.ClientError):
        load_data_to_s3.df_to_s3(DF, "k.csv", "my-bucket", "x", "y",
                                 expected_bucket_owner=OWNER)
    out = capsys.readouterr().out
    assert "Failed to upload" in out and "Uploaded" not in out


@pytest.mark.parametrize("owner", [None, "", "12345", "1111222233334", "abcdefghijkl"])
def test_invalid_expected_owner_rejected_before_upload(owner):
    with mock.patch.object(load_data_to_s3, "connect_to_s3") as connect:
        with pytest.raises(ValueError):
            load_data_to_s3.df_to_s3(DF, "k.csv", "my-bucket", "x", "y",
                                     expected_bucket_owner=owner)
    connect.assert_not_called()


def test_expected_owner_is_required():
    with pytest.raises(TypeError):
        load_data_to_s3.df_to_s3(DF, "k.csv", "my-bucket", "x", "y")


@pytest.mark.parametrize("env", [
    {},
    {"S3_BUCKET": "my-bucket"},
    {"S3_EXPECTED_BUCKET_OWNER": OWNER},
    {"S3_BUCKET": "my-bucket", "S3_EXPECTED_BUCKET_OWNER": "not-an-id"},
])
def test_main_fails_at_startup_without_s3_config(monkeypatch, env):
    for name in ("S3_BUCKET", "S3_EXPECTED_BUCKET_OWNER", "S3_KEY"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(main, "load_dotenv", lambda: None)
    with mock.patch.object(main, "extract_vehicle_sales_data") as extract:
        with pytest.raises(SystemExit) as exc:
            main.main()
    assert exc.value.code not in (0, None)
    extract.assert_not_called()


def test_main_exits_nonzero_without_success_message_when_upload_rejected(tmp_path):
    runner = tmp_path / "run.py"
    runner.write_text(
        "import runpy, sys, boto3, pandas as pd\n"
        "from botocore.stub import Stubber\n"
        f"sys.path.insert(0, {str(REPO)!r})\n"
        "import src.extract\n"
        "src.extract.extract_vehicle_sales_data = lambda *a: pd.DataFrame({'vin': ['V1']})\n"
        "client = boto3.client('s3', region_name='us-west-2',"
        " aws_access_key_id='x', aws_secret_access_key='y')\n"
        "stub = Stubber(client)\n"
        "stub.add_client_error('put_object', service_error_code='AccessDenied',"
        " http_status_code=403, expected_params={'Bucket': 'my-bucket',"
        " 'Key': 'auto_oem/etl/vehicle_sales_deduped.csv',"
        " 'Body': 'vin\\nV1\\n', 'ExpectedBucketOwner': '444455556666'})\n"
        "stub.activate()\n"
        "boto3.client = lambda *a, **k: client\n"
        f"runpy.run_path({str(REPO / 'main.py')!r}, run_name='__main__')\n"
    )
    env = {"PATH": "/usr/bin:/bin", "S3_BUCKET": "my-bucket",
           "S3_EXPECTED_BUCKET_OWNER": "444455556666"}
    proc = subprocess.run([sys.executable, str(runner)], cwd=tmp_path, env=env,
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode != 0
    assert "AccessDenied" in proc.stdout + proc.stderr
    assert "successfully uploaded" not in proc.stdout

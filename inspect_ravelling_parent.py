
import boto3
import pyarrow.parquet as pq
from io import BytesIO

BUCKET = "dhms-new-stgae"

KEY = (
    "DHMS/NSV/Silver/Zone=B/Phase=1/Cycle=1/"
    "UPC=N_02015_03002_GJ/dlm_kpi_new/"
    "master_ravelling.parquet"
)

TARGET_DEFECT = (
    "D/230125/N/N_02015_03002_GJ/255000/Ravelling/0051"
)

s3 = boto3.client("s3")

obj = s3.get_object(Bucket=BUCKET, Key=KEY)
table = pq.read_table(BytesIO(obj["Body"].read()))
df = table.to_pandas()

print("TOTAL PARENT ROWS:", len(df))
print("\nAVAILABLE COLUMNS:")
print(df.columns.tolist())

print("\nTHRESHOLD NAME DISTRIBUTION:")
if "threshold_name" in df.columns:
    print(df["threshold_name"].value_counts(dropna=False).to_string())

print("\nROWS WITH SAME DEFECT ID:")
if "defect_id" in df.columns:
    matched = df[
        df["defect_id"].astype(str).str.strip().eq(TARGET_DEFECT)
    ]

    print("Matching rows:", len(matched))

    if not matched.empty:
        print(matched.to_string(index=False))
    else:
        print("No parent record exists for this defect ID.")

print("\nFIRST 10 PARENT RECORDS:")
print(df.head(10).to_string(index=False))

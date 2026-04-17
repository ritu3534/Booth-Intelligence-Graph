import json, subprocess, sys

# Step 1: call the train API
result = subprocess.run(
    ["curl.exe", "-X", "POST", "http://localhost:7000/api/ml/train",
     "-H", "Content-Type: application/json", "-d", "{}", "-s"],
    capture_output=True, text=True
)

try:
    d = json.loads(result.stdout)
except Exception as e:
    print("API call failed:", result.stdout[:400], result.stderr[:200])
    sys.exit(1)

print("=" * 56)
print("  RETRAIN RESULTS — New Logic-Driven Dataset")
print("=" * 56)
print(f"  Status     : {d.get('status', 'N/A')}")
print(f"  Accuracy   : {round(d.get('accuracy', 0)*100, 2)}%")
print(f"  F1 Score   : {d.get('f1_score', 'N/A')}")
print(f"  ROC-AUC    : {d.get('roc_auc', 'N/A')}")
print(f"  Train size : {d.get('train_size', 'N/A')}")
print(f"  Test size  : {d.get('test_size', 'N/A')}")

cb = d.get("class_balance", {})
total = cb.get("floating", 0) + cb.get("committed", 0)
if total:
    print(f"  Floating   : {cb['floating']} ({cb['floating']/total*100:.1f}%)")
    print(f"  Committed  : {cb['committed']} ({cb['committed']/total*100:.1f}%)")

print()
print("  TOP 10 FEATURE IMPORTANCES:")
print("  " + "-" * 50)
for i, f in enumerate(d.get("feature_importances", [])[:10]):
    bar = "#" * int(f["importance"] * 50)
    print(f"  {i+1:2}. {f['feature']:<30} {f['importance']*100:5.2f}%  {bar}")
print("=" * 56)

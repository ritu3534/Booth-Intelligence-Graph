import json, sys

with open("ml_insights.json", encoding="utf-8") as f:
    d = json.load(f)

print("=" * 50)
print("   RANDOM FOREST MODEL REPORT — JanSetu AI")
print("=" * 50)
print(f"  Target Variable  : {d['target']}")
print(f"  Training Rows    : {d['train_size']}")
print(f"  Test Rows        : {d['test_size']}")
print(f"  N Estimators     : {d['n_estimators']} trees")
print(f"  Accuracy         : {d['accuracy']*100:.1f}%")
print(f"  F1 Score         : {d['f1_score']}")
print(f"  ROC-AUC          : {d['roc_auc']}")
print()
print("  Class Balance:")
print(f"    Floating Voters  : {d['class_balance']['floating']}")
print(f"    Committed Voters : {d['class_balance']['committed']}")
print()
print("  Top 10 Feature Importances:")
print("  " + "-" * 44)
for i, f in enumerate(d["feature_importances"][:10]):
    bar = "█" * int(f["importance"] * 200)
    print(f"  {i+1:2}. {f['feature']:25s} {f['importance']*100:5.2f}%  {bar}")
print("=" * 50)

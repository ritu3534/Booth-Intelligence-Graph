import json

d = json.load(open("tune_results.json", encoding="utf-8"))

print("=" * 58)
print("  RANDOM FOREST TUNING REPORT  -  JanSetu AI")
print("=" * 58)

print()
print("  SEARCH CONFIG")
print(f"    Candidates tested : {d['n_candidates']} random param combos")
print(f"    Cross-validation  : {d['cv_folds']}-fold stratified CV")
print(f"    Total fits        : {d['total_fits']} ({d['n_candidates']} x {d['cv_folds']})")
print(f"    Search time       : {d['search_time_seconds']}s")
print(f"    Best CV AUC       : {d['cv_best_auc']}")

print()
print("  DATASET SUMMARY")
print(f"    Train rows        : {d['train_size']}")
print(f"    Test rows         : {d['test_size']}")
print(f"    Floating voters   : {d['class_balance']['floating']}")
print(f"    Committed voters  : {d['class_balance']['committed']}")

print()
print("  METRIC COMPARISON")
print(f"    {'':20s} {'Baseline':>10} {'Tuned':>10} {'Delta':>10}")
print("    " + "-" * 54)
b, t, imp = d["baseline"], d["tuned"], d["improvement"]
print(f"    {'Accuracy':<20} {b['accuracy']*100:>9.2f}% {t['accuracy']*100:>9.2f}%  {imp['accuracy']:>+8.2f}%")
print(f"    {'F1 Score':<20} {b['f1_score']:>10.4f} {t['f1_score']:>10.4f}  {imp['f1_score']:>+8.4f}")
print(f"    {'ROC-AUC':<20} {b['roc_auc']:>10.4f} {t['roc_auc']:>10.4f}  {imp['roc_auc']:>+8.4f}")

print()
print("  BEST HYPERPARAMETERS")
for k, v in sorted(d["best_params"].items()):
    print(f"    {k:<25} : {v}")

print()
print("  FEATURE IMPORTANCES (Top 10)")
print("    " + "-" * 50)
for i, f in enumerate(d["feature_importances"][:10]):
    bar = "#" * int(f["importance"] * 40)
    print(f"    {i+1:2}. {f['feature']:<25} {f['importance']*100:5.2f}%  {bar}")

print()
cr = d["classification_report"]
print("  CLASSIFICATION REPORT (Tuned Model)")
print(f"    Precision (committed) : {cr['committed']['precision']:.4f}")
print(f"    Recall    (committed) : {cr['committed']['recall']:.4f}")
print(f"    Precision (floating)  : {cr['floating']['precision']:.4f}")
print(f"    Recall    (floating)  : {cr['floating']['recall']:.4f}")

api = d["api_retrain"]
print()
status = "SUCCESS" if api["success"] else "FAILED"
print(f"  LIVE API RETRAIN: {status}")
if api["success"]:
    r = api["result"]
    print(f"    Accuracy : {r.get('accuracy', 'N/A')}")
    print(f"    F1 Score : {r.get('f1_score', 'N/A')}")
    print(f"    ROC-AUC  : {r.get('roc_auc', 'N/A')}")

print("=" * 58)

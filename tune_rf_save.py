"""
tune_rf_save.py — RF Tuning that saves results to tune_results.json
"""
import sys, io, json, warnings, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import numpy as np
import pandas as pd
import requests
from pathlib import Path

from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import StratifiedShuffleSplit, RandomizedSearchCV, StratifiedKFold
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, classification_report

warnings.filterwarnings("ignore")

CSV_PATH    = Path("voters_data.csv")
API_BASE    = "http://localhost:7000"
RANDOM_SEED = 42
N_ITER      = 50
CV_FOLDS    = 5
TEST_SIZE   = 0.2

_CAT_COLS  = ["gender", "caste_category", "occupation"]
_BOOL_COLS = ["aadhaar_linked","has_lpg","has_bank_account","has_pucca_house",
              "is_income_taxpayer","ration_card","loan_defaulter","is_floating_node"]
_NUM_COLS  = ["age","monthly_income","land_holding_acres","pension_amount"]

def _bool_to_int(val):
    if isinstance(val, bool): return int(val)
    return 1 if str(val).strip().lower() in ("true","1","yes") else 0

# ── Load & Preprocess ─────────────────────────────────────────────────────────
print("Loading voters_data.csv ...")
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
target_series = df["is_floating_node"].apply(_bool_to_int)
feature_bool_cols = [c for c in _BOOL_COLS if c != "is_floating_node"]
for col in feature_bool_cols:
    df[col] = df[col].apply(_bool_to_int)
for col in _NUM_COLS:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
label_encoders = {}
for col in _CAT_COLS:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    label_encoders[col] = le

feature_cols = _NUM_COLS + feature_bool_cols + _CAT_COLS
X = df[feature_cols].values
y = target_series.values
print(f"Data shape: {X.shape}, Floating={y.sum()}, Committed={(y==0).sum()}")

# ── Split ─────────────────────────────────────────────────────────────────────
sss = StratifiedShuffleSplit(n_splits=1, test_size=TEST_SIZE, random_state=RANDOM_SEED)
for ti, vi in sss.split(X, y):
    X_train, X_test = X[ti], X[vi]
    y_train, y_test = y[ti], y[vi]

# ── Baseline ──────────────────────────────────────────────────────────────────
print("Training baseline model ...")
baseline = RandomForestClassifier(n_estimators=200, max_depth=None, min_samples_leaf=2,
                                   class_weight="balanced", random_state=RANDOM_SEED, n_jobs=-1)
baseline.fit(X_train, y_train)
yp_b  = baseline.predict(X_test)
ypr_b = baseline.predict_proba(X_test)[:,1]
acc_b, f1_b, auc_b = accuracy_score(y_test,yp_b), f1_score(y_test,yp_b,zero_division=0), roc_auc_score(y_test,ypr_b)
print(f"Baseline -> Acc={acc_b:.4f} F1={f1_b:.4f} AUC={auc_b:.4f}")

# ── RandomizedSearchCV ────────────────────────────────────────────────────────
param_dist = {
    "n_estimators":      [100, 200, 300, 500, 700],
    "max_depth":         [None, 5, 10, 15, 20, 30],
    "min_samples_split": [2, 5, 10, 20],
    "min_samples_leaf":  [1, 2, 4, 8],
    "max_features":      ["sqrt", "log2", 0.5, 0.7],
    "bootstrap":         [True, False],
    "class_weight":      ["balanced", "balanced_subsample"],
    "criterion":         ["gini", "entropy"],
}

print(f"Running RandomizedSearchCV ({N_ITER} iters x {CV_FOLDS} folds = {N_ITER*CV_FOLDS} fits) ...")
cv = StratifiedKFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)
rscv = RandomizedSearchCV(
    RandomForestClassifier(random_state=RANDOM_SEED, n_jobs=-1),
    param_distributions=param_dist, n_iter=N_ITER,
    scoring="roc_auc", cv=cv, verbose=0,
    random_state=RANDOM_SEED, n_jobs=-1, refit=True
)
t0 = time.time()
rscv.fit(X_train, y_train)
elapsed = time.time() - t0
print(f"Search done in {elapsed:.1f}s  |  Best CV AUC = {rscv.best_score_:.4f}")

# ── Evaluate tuned model ──────────────────────────────────────────────────────
best_clf = rscv.best_estimator_
yp_t  = best_clf.predict(X_test)
ypr_t = best_clf.predict_proba(X_test)[:,1]
acc_t, f1_t, auc_t = accuracy_score(y_test,yp_t), f1_score(y_test,yp_t,zero_division=0), roc_auc_score(y_test,ypr_t)
print(f"Tuned  -> Acc={acc_t:.4f} F1={f1_t:.4f} AUC={auc_t:.4f}")

# ── Classification Report ─────────────────────────────────────────────────────
cr = classification_report(y_test, yp_t, target_names=["committed","floating"], output_dict=True)

# ── Feature Importances ───────────────────────────────────────────────────────
fi_sorted = sorted(zip(feature_cols, best_clf.feature_importances_), key=lambda x: x[1], reverse=True)

# ── Push to API ───────────────────────────────────────────────────────────────
bp = rscv.best_params_
api_payload = {"n_estimators": bp["n_estimators"], "max_depth": bp["max_depth"],
               "min_samples_leaf": bp["min_samples_leaf"], "test_size": TEST_SIZE}
print(f"Pushing to API: {api_payload}")
try:
    resp = requests.post(f"{API_BASE}/api/ml/train", json=api_payload, timeout=60)
    api_ok = resp.status_code == 200
    api_result = resp.json() if api_ok else {"error": resp.text[:200]}
except Exception as e:
    api_ok = False
    api_result = {"error": str(e)}

# ── Save results ──────────────────────────────────────────────────────────────
results = {
    "baseline": {"accuracy": round(acc_b,4), "f1_score": round(f1_b,4), "roc_auc": round(auc_b,4)},
    "tuned":    {"accuracy": round(acc_t,4), "f1_score": round(f1_t,4), "roc_auc": round(auc_t,4)},
    "improvement": {
        "accuracy": round((acc_t-acc_b)*100,2),
        "f1_score": round(f1_t-f1_b,4),
        "roc_auc":  round(auc_t-auc_b,4),
    },
    "best_params": {k: (None if v is None else v) for k,v in rscv.best_params_.items()},
    "cv_best_auc": round(rscv.best_score_, 4),
    "search_time_seconds": round(elapsed, 1),
    "n_candidates": N_ITER,
    "cv_folds": CV_FOLDS,
    "total_fits": N_ITER * CV_FOLDS,
    "class_balance": {"floating": int(y.sum()), "committed": int((y==0).sum())},
    "train_size": len(X_train),
    "test_size":  len(X_test),
    "feature_importances": [{"feature": f, "importance": round(float(i),4)} for f,i in fi_sorted],
    "classification_report": cr,
    "api_retrain": {"success": api_ok, "result": api_result},
}

with open("tune_results.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2)

print("Results saved to tune_results.json")

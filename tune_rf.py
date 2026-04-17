# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
"""
tune_rf.py — Random Forest Hyperparameter Tuning for JanSetu AI
================================================================
Steps performed:
  1. Load & preprocess  voters_data.csv  (same pipeline as dashboard.py)
  2. Run RandomizedSearchCV (50 candidates, 5-fold stratified CV)
     optimised for ROC-AUC
  3. Print the best hyperparameters & cross-validated scores
  4. Retrain on the FULL training set with those best params
  5. Evaluate on the hold-out test set
  6. POST   /api/ml/train   with the best params so the live API
     also uses the tuned model
"""

import json, warnings, time
import numpy as np
import pandas as pd
import requests
from pathlib import Path

from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import (
    StratifiedShuffleSplit,
    RandomizedSearchCV,
    StratifiedKFold,
)
from sklearn.metrics import (
    accuracy_score, f1_score, roc_auc_score, classification_report
)

warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────
CSV_PATH   = Path("voters_data.csv")
API_BASE   = "http://localhost:7000"
RANDOM_SEED = 42
N_ITER      = 50      # number of random param combinations to try
CV_FOLDS    = 5       # stratified k-fold splits
TEST_SIZE   = 0.2     # hold-out split

_CAT_COLS  = ["gender", "caste_category", "occupation"]
_BOOL_COLS = [
    "aadhaar_linked", "has_lpg", "has_bank_account",
    "has_pucca_house", "is_income_taxpayer", "ration_card",
    "loan_defaulter", "is_floating_node",
]
_NUM_COLS = ["age", "monthly_income", "land_holding_acres", "pension_amount"]


def _bool_to_int(val) -> int:
    if isinstance(val, bool):
        return int(val)
    return 1 if str(val).strip().lower() in ("true", "1", "yes") else 0


def sep(char="=", n=56):
    print(char * n)


# ── Step 1: Load & Preprocess ─────────────────────────────────────────────────
sep()
print("  STEP 1 — Loading voters_data.csv …")
sep()

df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
print(f"  Rows loaded        : {len(df)}")
print(f"  Columns            : {list(df.columns)}")

target_series = df["is_floating_node"].apply(_bool_to_int)
print(f"\n  Target distribution:")
print(f"    Floating  (1)  : {target_series.sum()}   ({target_series.mean()*100:.1f}%)")
print(f"    Committed (0)  : {(target_series==0).sum()}  ({(1-target_series.mean())*100:.1f}%)")

# Encode booleans (exclude target)
feature_bool_cols = [c for c in _BOOL_COLS if c != "is_floating_node"]
for col in feature_bool_cols:
    df[col] = df[col].apply(_bool_to_int)

# Encode numerics
for col in _NUM_COLS:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# Label-encode categoricals
label_encoders = {}
for col in _CAT_COLS:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    label_encoders[col] = le
    print(f"  Encoded '{col}' → {len(le.classes_)} categories: {list(le.classes_)}")

feature_cols = _NUM_COLS + feature_bool_cols + _CAT_COLS
X = df[feature_cols].values
y = target_series.values

print(f"\n  Feature matrix     : {X.shape[0]} rows × {X.shape[1]} features")
print(f"  Features used      : {feature_cols}")


# ── Step 2: Train/Test Split ──────────────────────────────────────────────────
sep()
print("  STEP 2 — Stratified Train/Test Split …")
sep()

sss = StratifiedShuffleSplit(n_splits=1, test_size=TEST_SIZE, random_state=RANDOM_SEED)
for train_idx, test_idx in sss.split(X, y):
    X_train, X_test = X[train_idx], X[test_idx]
    y_train, y_test = y[train_idx], y[test_idx]

print(f"  Train size : {len(X_train)} rows  (floating={y_train.sum()}, committed={(y_train==0).sum()})")
print(f"  Test  size : {len(X_test)} rows  (floating={y_test.sum()}, committed={(y_test==0).sum()})")


# ── Step 3: Baseline (current model defaults) ─────────────────────────────────
sep()
print("  STEP 3 — Baseline Model (default params) …")
sep()

baseline = RandomForestClassifier(
    n_estimators=200,
    max_depth=None,
    min_samples_leaf=2,
    class_weight="balanced",
    random_state=RANDOM_SEED,
    n_jobs=-1,
)
baseline.fit(X_train, y_train)
y_pred_b  = baseline.predict(X_test)
y_prob_b  = baseline.predict_proba(X_test)[:, 1]

print(f"  Accuracy  : {accuracy_score(y_test, y_pred_b)*100:.2f}%")
print(f"  F1 Score  : {f1_score(y_test, y_pred_b, zero_division=0):.4f}")
print(f"  ROC-AUC   : {roc_auc_score(y_test, y_prob_b):.4f}")


# ── Step 4: RandomizedSearchCV ────────────────────────────────────────────────
sep()
print(f"  STEP 4 — RandomizedSearchCV ({N_ITER} candidates, {CV_FOLDS}-fold CV) …")
print("  This may take 30–60 seconds …")
sep()

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

cv = StratifiedKFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)

rscv = RandomizedSearchCV(
    RandomForestClassifier(random_state=RANDOM_SEED, n_jobs=-1),
    param_distributions=param_dist,
    n_iter=N_ITER,
    scoring="roc_auc",
    cv=cv,
    verbose=1,
    random_state=RANDOM_SEED,
    n_jobs=-1,
    refit=True,
)

t0 = time.time()
rscv.fit(X_train, y_train)
elapsed = time.time() - t0

print(f"\n  Search completed in {elapsed:.1f}s")
print(f"  Best CV ROC-AUC   : {rscv.best_score_:.4f}")
print(f"\n  Best Hyperparameters:")
for k, v in sorted(rscv.best_params_.items()):
    print(f"    {k:25s}: {v}")


# ── Step 5: Evaluate Best Model on Hold-out Set ───────────────────────────────
sep()
print("  STEP 5 — Evaluating tuned model on hold-out test set …")
sep()

best_clf = rscv.best_estimator_
y_pred_t = best_clf.predict(X_test)
y_prob_t = best_clf.predict_proba(X_test)[:, 1]

acc_t  = accuracy_score(y_test, y_pred_t)
f1_t   = f1_score(y_test, y_pred_t, zero_division=0)
auc_t  = roc_auc_score(y_test, y_prob_t)
acc_b  = accuracy_score(y_test, y_pred_b)
f1_b   = f1_score(y_test, y_pred_b, zero_division=0)
auc_b  = roc_auc_score(y_test, y_prob_b)

print(f"\n  {'Metric':<15} {'Baseline':>12} {'Tuned':>12} {'Improvement':>14}")
print("  " + "-" * 55)
print(f"  {'Accuracy':<15} {acc_b*100:>11.2f}% {acc_t*100:>11.2f}%  {(acc_t-acc_b)*100:>+10.2f}%")
print(f"  {'F1 Score':<15} {f1_b:>12.4f} {f1_t:>12.4f}  {(f1_t-f1_b):>+10.4f}")
print(f"  {'ROC-AUC':<15} {auc_b:>12.4f} {auc_t:>12.4f}  {(auc_t-auc_b):>+10.4f}")

print(f"\n  Full Classification Report (Tuned Model):")
print(classification_report(y_test, y_pred_t, target_names=["committed", "floating"]))

print("\n  Feature Importances (Tuned Model, Top 10):")
print("  " + "-" * 52)
fi_sorted = sorted(zip(feature_cols, best_clf.feature_importances_), key=lambda x: x[1], reverse=True)
for i, (feat, imp) in enumerate(fi_sorted[:10]):
    bar = "#" * int(imp * 40)
    print(f"  {i+1:2}. {feat:25s} {imp*100:5.2f}%  {bar}")


# ── Step 6: Push tuned params to live API ─────────────────────────────────────
sep()
print("  STEP 6 — Pushing tuned model to live API …")
sep()

bp = rscv.best_params_
api_payload = {
    "n_estimators":     bp["n_estimators"],
    "max_depth":        bp["max_depth"],
    "min_samples_leaf": bp["min_samples_leaf"],
    "test_size":        TEST_SIZE,
}

print(f"  POSTing to {API_BASE}/api/ml/train with:")
print(f"    {json.dumps(api_payload, indent=6)}")

try:
    resp = requests.post(f"{API_BASE}/api/ml/train", json=api_payload, timeout=60)
    if resp.status_code == 200:
        result = resp.json()
        print(f"\n  ✅ Live API retrained successfully!")
        print(f"     Accuracy  : {result.get('accuracy', 'N/A')}")
        print(f"     F1 Score  : {result.get('f1_score', 'N/A')}")
        print(f"     ROC-AUC   : {result.get('roc_auc', 'N/A')}")
    else:
        print(f"  ⚠️  API returned {resp.status_code}: {resp.text[:200]}")
except Exception as e:
    print(f"  ⚠️  Could not reach API: {e}")


sep()
print("  ✅  TUNING COMPLETE")
sep("=")

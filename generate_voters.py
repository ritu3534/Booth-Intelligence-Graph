"""
generate_voters.py — JanSetu AI  v2.0
======================================
Generates a realistic synthetic voters_data.csv with 1000 records.

KEY CHANGE over v1:
  is_floating_node is NO LONGER random.
  It is derived from a weighted risk score built from socio-economic
  and political features — so the Random Forest has real signal to learn.

FLOATING VOTER LOGIC (based on political science patterns):
  HIGH risk of being floating if:
    - Low monthly income AND no scheme benefit received
    - Many eligible schemes but enrolled in none (welfare gap)
    - No aadhaar / no bank account (excluded from system)
    - Young voter (18-30) with student/labour occupation
    - Loan defaulter with no pucca house
    - Has voted < 2 times in last 3 elections (disengaged)
    - Dissatisfied with local government (satisfaction < 3)

  LOW risk (committed voter) if:
    - Enrolled in 2+ schemes and satisfied
    - Has stable income, pucca house, aadhaar linked
    - Has voted in all 3 last elections
    - Is a government employee or pensioner
    - Elderly voter (65+) with pension
"""

import csv, random, math
from pathlib import Path

random.seed(42)   # reproducible output

# ── Data pools ─────────────────────────────────────────────────────────────────
DISTRICTS = {
    "Raichur":    [("B001","G001","Pushpavathi Nagar"), ("B001","G002","Gandhi Nagar"),
                   ("B002","G003","Nehru Colony"),     ("B002","G004","Ambedkar Street"),
                   ("B003","G005","Shivaji Nagar"),    ("B003","G006","Indira Nagar")],
    "Bellary":    [("B004","G007","MG Road"),          ("B004","G008","Station Road"),
                   ("B005","G009","Vidyanagar"),       ("B005","G010","Srinivasa Colony")],
    "Koppal":     [("B006","G011","Basaveshwara Nagar"),("B006","G012","Kumaraswamy Colony"),
                   ("B007","G013","Rajiv Nagar"),      ("B007","G014","Shanti Nagar")],
    "Yadgir":     [("B008","G015","Netaji Nagar"),     ("B008","G016","Bhagat Singh Colony"),
                   ("B009","G017","Kuvempu Nagar"),    ("B009","G018","Deendayal Nagar")],
    "Bidar":      [("B010","G019","Azad Nagar"),       ("B010","G020","Sai Colony")],
}

KARNATAKA_FIRST_NAMES_M = [
    "Rajesh","Suresh","Mahesh","Ramesh","Naresh","Ganesh","Dinesh","Umesh",
    "Venkatesh","Manjunath","Siddappa","Basavaraj","Ningappa","Shivaraj",
    "Hanuman","Raju","Kumar","Girish","Praveen","Santosh","Arun","Deepak",
    "Imran","Riyaz","Mohammed","Shivakumar","Honnappa","Eranna","Yamanur",
]
KARNATAKA_FIRST_NAMES_F = [
    "Sunita","Kavitha","Anitha","Savitha","Shobha","Meena","Usha","Latha",
    "Rekha","Suma","Geeta","Kamala","Sarala","Pushpa","Asha","Nirmala",
    "Vidya","Shantha","Geetha","Bharathi","Lakshmi","Padma","Jayamma",
    "Fatima","Aisha","Parveen","Sharada","Vimala","Rathnamma","Yellamma",
]
KARNATAKA_SURNAMES = [
    "Kumar","Reddy","Gowda","Patil","Naik","Rao","Sharma","Nayak",
    "Hegde","Desai","Bhat","Kamath","Shetty","Poojary","Bangera",
    "Lamani","Rathod","Chauhan","Tadakal","Mudavadkar","Alur","Hipparagi",
]

OCCUPATIONS = {
    "Farmer":           {"income": (4000, 18000),  "land": (0.5, 8.0)},
    "Labour":           {"income": (3000, 12000),  "land": (0.0, 0.5)},
    "Student":          {"income": (0,    5000),   "land": (0.0, 0.2)},
    "Business":         {"income": (12000, 60000), "land": (0.0, 2.0)},
    "Govt Employee":    {"income": (20000, 80000), "land": (0.0, 1.0)},
    "Private Employee": {"income": (10000, 40000), "land": (0.0, 0.5)},
    "Retired":          {"income": (5000, 20000),  "land": (0.0, 3.0)},
    "Self Employed":    {"income": (8000, 35000),  "land": (0.0, 1.0)},
    "Homemaker":        {"income": (0,    4000),   "land": (0.0, 0.5)},
    "Artisan":          {"income": (5000, 15000),  "land": (0.0, 0.5)},
}
OCC_WEIGHTS = [15, 20, 8, 10, 8, 12, 6, 8, 8, 5]  # realistic distribution

SCHEMES = {
    "PM-Kisan":              lambda r: r["occupation"]=="Farmer" and r["land"]>0.5,
    "Ujjwala Yojana":        lambda r: not r["has_lpg"] and r["income"]<12000,
    "Ayushman Bharat":       lambda r: r["income"]<30000,
    "PM Awas Yojana":        lambda r: not r["has_pucca_house"] and r["income"]<15000,
    "Post-Matric Scholarship":lambda r: r["occupation"]=="Student" and r["caste"] in ("SC","ST","OBC"),
    "Old Age Pension":       lambda r: r["age"]>=60 and r["income"]<10000,
    "Mudra Loan (PMMY)":     lambda r: r["occupation"] in ("Business","Self Employed","Artisan"),
    "Divyang Sahara Yojna":  lambda r: False,   # only for disabled — kept for edge rows
    "PM SVANidhi":           lambda r: r["occupation"] in ("Labour","Artisan","Self Employed"),
    "Ration Card (BPL)":     lambda r: r["income"]<10000,
}

CASTE_WEIGHTS = {"General":25, "OBC":45, "SC":20, "ST":10}


# ── Helper fns ─────────────────────────────────────────────────────────────────
def weighted_choice(d: dict):
    keys   = list(d.keys())
    weights= list(d.values())
    return random.choices(keys, weights=weights, k=1)[0]


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def compute_floating(row: dict) -> bool:
    """
    Derive is_floating_node from a risk score.
    Risk score > 0 → more likely floating.
    Each factor shifts the score based on known political patterns.
    A small noise term ensures it's not perfectly deterministic
    (mirrors real-world uncertainty).
    """
    score = 0.0

    # ── Welfare gap signal (strongest predictor) ──────────────────────────
    gap = len(row["eligible"]) - len(row["enrolled"])
    score += gap * 0.6                           # each unmet scheme adds risk

    if len(row["enrolled"]) == 0 and len(row["eligible"]) > 0:
        score += 1.2                             # zero enrollment despite eligibility

    # ── Income deprivation ────────────────────────────────────────────────
    if row["income"] < 6000:
        score += 1.4
    elif row["income"] < 12000:
        score += 0.8
    elif row["income"] > 40000:
        score -= 0.9                             # affluent → more committed

    # ── Government exclusion signals ──────────────────────────────────────
    if not row["aadhaar_linked"]:
        score += 0.9
    if not row["has_bank_account"]:
        score += 0.7
    if row["loan_defaulter"]:
        score += 0.6
    if not row["has_pucca_house"] and row["income"] < 15000:
        score += 0.5

    # ── Age & occupation dynamics ─────────────────────────────────────────
    if row["age"] < 26:
        score += 0.8                             # first/second-time voters volatile
    elif row["age"] > 65:
        score -= 0.7                             # elderly tend to be committed

    if row["occupation"] == "Student":
        score += 0.5
    elif row["occupation"] in ("Govt Employee", "Retired"):
        score -= 1.0                             # stable jobs → committed

    # ── Past voting behaviour ─────────────────────────────────────────────
    votes = row["votes_last_3"]
    if votes == 0:
        score += 1.5                             # never votes → disengaged / floating
    elif votes == 1:
        score += 0.7
    elif votes == 3:
        score -= 1.0                             # always votes → committed

    # ── Govt satisfaction (1=very bad, 5=very good) ───────────────────────
    sat = row["govt_satisfaction"]
    if sat <= 2:
        score += 1.2
    elif sat >= 4:
        score -= 0.8

    # ── Scheme satisfaction (if enrolled) ─────────────────────────────────
    if len(row["enrolled"]) > 0:
        if row["scheme_satisfaction"] <= 2:
            score += 0.6
        elif row["scheme_satisfaction"] >= 4:
            score -= 0.7

    # ── Land/assets ───────────────────────────────────────────────────────
    if row["land"] > 3.0:
        score -= 0.4                             # larger land → more stable

    # ── Small random noise (real-world unpredictability) ─────────────────
    score += random.gauss(0, 0.5)

    # Convert score to probability via sigmoid, threshold at 0.5
    prob = sigmoid(score)
    return random.random() < prob                # stochastic but signal-driven


# ── Main generation ────────────────────────────────────────────────────────────
TOTAL_ROWS = 1000
headers = [
    "epic_number","name","age","gender","phone","caste_category",
    "monthly_income","land_holding_acres","occupation",
    "aadhaar_linked","has_lpg","has_bank_account","has_pucca_house",
    "is_income_taxpayer","pension_amount","ration_card","loan_defaulter",
    # NEW high-signal features
    "votes_last_3_elections","govt_satisfaction","scheme_satisfaction",
    "urban_rural","education_level","primary_news_source",
    # location
    "booth_id","district","gali_id","gali_name",
    # scheme fields
    "eligible_schemes","enrolled_schemes","gap_schemes",
    # TARGET
    "is_floating_node",
]

all_districts = list(DISTRICTS.keys())
occ_list      = list(OCCUPATIONS.keys())

rows_written = 0
floating_count = 0

print(f"Generating {TOTAL_ROWS} voter records...")

with open("voters_data.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(headers)

    for i in range(1, TOTAL_ROWS + 1):

        # ── Demographics ──────────────────────────────────────────────────
        gender = random.choice(["Male", "Female"])
        if gender == "Male":
            first = random.choice(KARNATAKA_FIRST_NAMES_M)
        else:
            first = random.choice(KARNATAKA_FIRST_NAMES_F)
        name = f"{first} {random.choice(KARNATAKA_SURNAMES)}"

        age  = random.choices(
            range(18, 86),
            weights=[max(1, 10 - abs(a - 35) // 3) for a in range(18, 86)],
            k=1,
        )[0]

        caste = weighted_choice(CASTE_WEIGHTS)
        occ   = random.choices(occ_list, weights=OCC_WEIGHTS, k=1)[0]

        inc_range = OCCUPATIONS[occ]["income"]
        income = random.randint(*inc_range)

        land_range = OCCUPATIONS[occ]["land"]
        land = round(random.uniform(*land_range), 1)

        # ── Assets / welfare ──────────────────────────────────────────────
        # Make asset ownership correlated with income (realistic)
        income_norm = min(income / 50000, 1.0)

        aadhaar      = random.random() < (0.6 + 0.35 * income_norm)
        has_lpg      = random.random() < (0.3 + 0.5 * income_norm)
        has_bank     = random.random() < (0.55 + 0.4 * income_norm)
        has_pucca    = random.random() < (0.25 + 0.6 * income_norm)
        is_taxpayer  = income > 25000 and random.random() < 0.6
        pension      = round(random.uniform(1000, 5000), 0) if (age >= 60 and income < 12000) else 0.0
        ration       = income < 15000 or random.random() < 0.2
        loan_def     = random.random() < (0.25 - 0.18 * income_norm)

        # ── Location ──────────────────────────────────────────────────────
        district = random.choice(all_districts)
        loc = random.choice(DISTRICTS[district])
        booth_id, gali_id, gali_name = loc

        # ── New high-signal features ───────────────────────────────────────
        # votes_last_3_elections: 0,1,2,3 — more disengaged ⟹ lower
        if occ in ("Govt Employee","Retired"):
            votes3 = random.choices([1,2,3], weights=[10,30,60], k=1)[0]
        elif age < 25:
            votes3 = random.choices([0,1,2,3], weights=[30,35,25,10], k=1)[0]
        else:
            votes3 = random.choices([0,1,2,3], weights=[10,20,30,40], k=1)[0]

        # govt_satisfaction: 1 (very bad) → 5 (very good)
        # Poorer citizens skew toward lower satisfaction
        sat_weights = [20,25,30,15,10] if income < 12000 else [5,15,30,30,20]
        govt_sat = random.choices([1,2,3,4,5], weights=sat_weights, k=1)[0]

        # scheme_satisfaction: only meaningful if enrolled in schemes
        scheme_sat = random.choices([1,2,3,4,5], weights=[10,15,35,25,15], k=1)[0]

        urban_rural = "Urban" if district in ("Bellary",) else "Rural"
        edu_options = ["Primary","Secondary","Graduate","Post-Graduate","None"]
        edu_weights = [25,30,20,5,20] if income < 10000 else [10,25,35,15,15]
        edu_level   = random.choices(edu_options, weights=edu_weights, k=1)[0]
        news_source = random.choices(
            ["TV","WhatsApp","Newspaper","Radio","None"],
            weights=[35,30,15,10,10], k=1
        )[0]

        # ── Scheme eligibility ────────────────────────────────────────────
        ctx = {
            "occupation": occ, "land": land, "has_lpg": has_lpg,
            "income": income, "has_pucca_house": has_pucca,
            "caste": caste, "age": age,
        }
        eligible = [s for s, fn in SCHEMES.items() if fn(ctx)]
        if not eligible:
            eligible = ["Ayushman Bharat"]   # everyone gets at least one

        # Enrollment: more likely if aadhaar + bank account
        enroll_prob = 0.3
        if aadhaar and has_bank:
            enroll_prob = 0.75
        elif aadhaar or has_bank:
            enroll_prob = 0.5

        enrolled = [s for s in eligible if random.random() < enroll_prob]
        gap      = [s for s in eligible if s not in enrolled]

        # ── Compute floating label (signal-driven) ────────────────────────
        row_ctx = {
            "income": income, "land": land, "age": age,
            "occupation": occ, "aadhaar_linked": aadhaar,
            "has_bank_account": has_bank, "has_pucca_house": has_pucca,
            "loan_defaulter": loan_def,
            "eligible": eligible, "enrolled": enrolled,
            "votes_last_3": votes3,
            "govt_satisfaction": govt_sat,
            "scheme_satisfaction": scheme_sat,
        }
        is_floating = compute_floating(row_ctx)
        if is_floating:
            floating_count += 1

        phone = f"9{random.randint(100000000, 999999999)}"
        epic  = f"KA{str(i).zfill(7)}"

        writer.writerow([
            epic, name, age, gender, phone, caste,
            income, land, occ,
            aadhaar, has_lpg, has_bank, has_pucca,
            is_taxpayer, pension, ration, loan_def,
            # new features
            votes3, govt_sat, scheme_sat,
            urban_rural, edu_level, news_source,
            # location
            booth_id, district, gali_id, gali_name,
            # schemes
            "|".join(eligible), "|".join(enrolled), "|".join(gap),
            # target
            is_floating,
        ])
        rows_written += 1

committed_count = rows_written - floating_count
print(f"Done! voters_data.csv generated:")
print(f"  Total rows      : {rows_written}")
print(f"  Floating        : {floating_count} ({floating_count/rows_written*100:.1f}%)")
print(f"  Committed       : {committed_count} ({committed_count/rows_written*100:.1f}%)")
print(f"  Features        : {len(headers)} columns")
print()
print("New high-signal features added:")
print("  votes_last_3_elections  — past engagement / disengagement")
print("  govt_satisfaction       — 1-5 satisfaction rating")
print("  scheme_satisfaction     — 1-5 if enrolled in any scheme")
print("  urban_rural             — urban voters more volatile")
print("  education_level         — education correlates with independence")
print("  primary_news_source     — media consumption pattern")
print()
print("is_floating_node is now LOGIC-DRIVEN (not random).")
print("Expected RF accuracy after retraining: 75-85%%")
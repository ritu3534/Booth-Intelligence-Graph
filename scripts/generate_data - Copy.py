"""
JanSetu AI — Synthetic Dataset Generator v2
Run: python Generate_data.py
Output: voter_list.csv (500 citizens across 20 booths, 5 streets each)

v2 adds:
  - gali_id   : street identifier  (e.g. G001-1)
  - gali_name : street name in Kannada/English mix (e.g. MG Road, Gandhi Nagar)
"""

import pandas as pd
import random

random.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
DISTRICTS  = ["Raichur", "Gulbarga"]
CASTES     = ["General", "OBC", "SC", "ST", "Minority"]
OCCUPATIONS= ["Farmer", "Labour", "Business", "Govt", "Professional", "Fisherman", "Animal Husbandry"]
GENDERS    = ["Male", "Female"]

KARNATAKA_EPIC_PREFIXES = ["KAR", "JGK", "RCR", "GBG", "BRJ", "KLG", "YDG", "CPT"]

KANNADA_MALE_NAMES = [
    "Raju Naik", "Basavraj Patil", "Manjunath Reddy", "Siddappa Goudar",
    "Hanumantha Rao", "Veeranna Lamani", "Channappa Hosamani", "Eranna Biradar",
    "Mallappa Talawar", "Shivappa Nayak", "Gurusiddappa Metri", "Nagappa Wali",
    "Parasappa Kamble", "Shekharappa Jadav", "Thippeswamy Gowda"
]
KANNADA_FEMALE_NAMES = [
    "Savitha Naik", "Renuka Patil", "Geetha Reddy", "Lakshmi Goudar",
    "Kavitha Rao", "Sharada Lamani", "Sumitra Hosamani", "Bhagya Biradar",
    "Yellamma Talawar", "Parvathi Nayak", "Nagaveni Metri", "Thulasi Wali",
    "Ambika Kamble", "Hemalatha Jadav", "Devaki Gowda"
]

# ── Gali (Street) names — realistic Karnataka street names ────────────────────
GALI_NAMES = [
    "MG Road", "Gandhi Nagar", "Nehru Colony", "Ambedkar Street",
    "Rajiv Nagar", "Indira Nagar", "Subhash Chowk", "Bhagat Singh Road",
    "Shivaji Nagar", "Patel Colony", "Vikas Nagar", "Sangolli Rayanna Road",
    "Basaveshwara Circle", "Kuvempu Nagar", "Narayana Swamy Street",
    "Anna Bhavani Road", "Azad Nagar", "Mallikarjun Colony",
    "Yellamma Devi Street", "Siddeshwara Nagar", "Veeresh Nagar",
    "Devendrappa Road", "Hanumantha Colony", "Ramaiah Street",
    "Nagenahalli Road", "Channamma Nagar", "Kittur Layout",
    "Valmiki Nagar", "Guru Basavanna Road", "Lalitha Nagar",
    "Shrinivasa Colony", "Eramma Street", "Holiyappa Road",
    "Komala Layout", "Basappa Nagar", "Thimmappa Street",
    "Shivakumar Road", "Nagendra Colony", "Mallappa Nagar",
    "Venkatesh Layout", "Malleshwara Colony", "Seshaiah Street",
    "Ramanjaneyya Road", "Hanumanth Nagar", "Krishnappa Layout",
    "Somashekar Colony", "Nanjappa Road", "Muttaiah Nagar",
    "Jayalakshmi Street", "Rangaiah Colony", "Govindaiah Road",
    "Shivarudraiah Nagar", "Muniswamy Layout", "Narasimhaiah Colony",
    "Venkataramaiah Street", "Siddalingaiah Road", "Muniyappa Nagar",
    "Papaiah Layout", "Thayamma Colony", "Kamakshi Street",
    "Lakshmamma Road", "Yellappa Nagar", "Siddamma Colony",
    "Bhagirathi Street", "Nagalakshmi Road", "Pushpavathi Nagar",
    "Saraswathi Colony", "Meenakshi Street", "Vijayalakshmi Road",
    "Rukminibai Nagar", "Vasanthi Colony", "Janaki Street",
    "Parvathamma Road", "Sharadamma Nagar", "Thulasamma Colony",
    "Geetha Street", "Kavitha Road", "Renuka Nagar",
    "Sumitra Colony", "Bhagya Street", "Savitha Road",
    "Lakshmi Nagar", "Yellamma Street", "Annapurna Colony",
    "Sharada Road", "Girija Nagar", "Mangalamma Colony",
    "Veeralakshmi Street", "Nagaveni Road", "Ambika Nagar",
    "Thippavva Colony", "Chandra Nagar", "Susheela Street",
    "Mahadevi Road", "Kalavati Nagar", "Sumangala Colony",
    "Nirmala Street", "Rathnamma Road", "Chandravathi Nagar",
    "Seethalakshmi Colony", "Sarojini Street", "Vasantha Road",
    "Indumathi Nagar", "Kamala Colony",
]


def generate_epic(index):
    prefix = random.choice(KARNATAKA_EPIC_PREFIXES)
    number = str(index).zfill(7)
    return f"{prefix}{number}"


def compute_eligibility(c):
    eligible = []
    if (c["land_holding_acres"] > 0 and c["aadhaar_linked"]
            and not c["is_income_taxpayer"]
            and c["occupation"] not in ["Doctor","Lawyer","Engineer","CA","Professional","Govt"]
            and c["pension_amount"] < 10000):
        eligible.append("PM Kisan Samman Nidhi")
    if (18 <= c["age"] <= 65
            and c["occupation"] in ["Business","Self-Employed","Labour"]
            and not c["loan_defaulter"] and c["has_bank_account"]):
        eligible.append("Mudra Loan (PMMY)")
    if (c["gender"] == "Female" and not c["has_lpg"]
            and c["monthly_income"] < 15000
            and c["caste_category"] in ["SC","ST","OBC","Minority"]):
        eligible.append("Ujjwala Yojana")
    if (c["land_holding_acres"] > 0
            or c["occupation"] in ["Fisherman","Animal Husbandry"]):
        eligible.append("Kisan Credit Card")
    if not c["has_bank_account"]:
        eligible.append("Jan Dhan Yojana")
    if not c["has_pucca_house"] and c["monthly_income"] < 18000:
        eligible.append("PM Awas Yojana")
    if c["land_holding_acres"] > 0:
        eligible.append("PM Fasal Bima")
    if c["ration_card"] or c["age"] >= 70 or c["monthly_income"] < 10000:
        eligible.append("Ayushman Bharat")
    return eligible


def compute_enrolled(eligible, c):
    enrolled = []
    probs = {
        "PM Kisan Samman Nidhi": 0.73,
        "Mudra Loan (PMMY)":     0.57,
        "Ujjwala Yojana":        0.77,
        "Kisan Credit Card":     0.60,
        "Jan Dhan Yojana":       0.81,
        "PM Awas Yojana":        0.51,
        "PM Fasal Bima":         0.46,
        "Ayushman Bharat":       0.62,
    }
    for scheme in eligible:
        if random.random() < probs.get(scheme, 0.60):
            enrolled.append(scheme)
    return enrolled


def generate_citizen(index, booth_id, district, gali_id, gali_name):
    gender     = random.choice(GENDERS)
    age        = random.randint(18, 85)
    occupation = random.choice(OCCUPATIONS)
    land       = round(random.uniform(0.5, 6.0), 1) if occupation in ["Farmer","Animal Husbandry"] else 0.0
    income     = random.randint(2500, 28000)
    caste      = random.choice(CASTES)

    name_pool   = KANNADA_MALE_NAMES if gender == "Male" else KANNADA_FEMALE_NAMES
    name        = random.choice(name_pool)
    unique_name = f"{name.split()[0]} {name.split()[1][0]}{index}"

    citizen = {
        "epic_number":        generate_epic(index),
        "name":               unique_name,
        "age":                age,
        "gender":             gender,
        "phone":              f"9{random.randint(100000000, 999999999)}",
        "caste_category":     caste,
        "monthly_income":     income,
        "land_holding_acres": land,
        "occupation":         occupation,
        "aadhaar_linked":     random.random() > 0.12,
        "has_lpg":            random.random() > 0.45,
        "has_bank_account":   random.random() > 0.18,
        "has_pucca_house":    random.random() > 0.48,
        "is_income_taxpayer": income > 22000 and random.random() > 0.65,
        "pension_amount":     random.choice([0,0,0,0,4000,8000,12000]),
        "ration_card":        random.random() > 0.28,
        "loan_defaulter":     random.random() > 0.92,
        "booth_id":           booth_id,
        "district":           district,
        # ── NEW: Gali (street) fields ──────────────────────────────────────
        "gali_id":            gali_id,
        "gali_name":          gali_name,
    }

    eligible = compute_eligibility(citizen)
    enrolled = compute_enrolled(eligible, citizen)
    citizen["eligible_schemes"] = "|".join(eligible)
    citizen["enrolled_schemes"] = "|".join(enrolled)
    citizen["gap_schemes"]      = "|".join(set(eligible) - set(enrolled))
    citizen["is_floating_node"] = len(set(eligible) - set(enrolled)) > 0
    return citizen


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    citizens  = []
    gali_pool = GALI_NAMES.copy()
    random.shuffle(gali_pool)
    gali_index = 0
    citizen_index = 1

    # 20 booths × 5 galis per booth × 5 citizens per gali = 500 citizens
    for b in range(1, 21):
        district = DISTRICTS[b % 2]
        booth_id = f"B{str(b).zfill(3)}"

        # Assign 5 unique galis to this booth
        booth_galis = []
        for g in range(1, 6):
            gali_id   = f"G{str(b).zfill(3)}-{g}"
            gali_name = gali_pool[gali_index % len(gali_pool)]
            gali_index += 1
            booth_galis.append((gali_id, gali_name))

        # 5 citizens per gali
        for gali_id, gali_name in booth_galis:
            for _ in range(5):
                c = generate_citizen(
                    citizen_index, booth_id, district, gali_id, gali_name
                )
                citizens.append(c)
                citizen_index += 1

    df = pd.DataFrame(citizens)
    df.to_csv("voter_list.csv", index=False, encoding="utf-8")

    print(f"\n✅ Generated {len(df)} citizens across 20 booths, 100 streets (galis)")
    print(f"   Floating nodes : {df['is_floating_node'].sum()}")
    print(f"\n📊 Sample EPIC numbers:")
    print(df["epic_number"].head(5).to_string(index=False))
    print(f"\n📊 Sample Galis:")
    print(df[["gali_id","gali_name","booth_id"]].drop_duplicates().head(10).to_string(index=False))
    print(f"\n💾 Saved to voter_list.csv")
import csv
import random

# Headers from your format
headers = [
    "epic_number", "name", "age", "gender", "phone", "caste_category", 
    "monthly_income", "land_holding_acres", "occupation", "aadhaar_linked", 
    "has_lpg", "has_bank_account", "has_pucca_house", "is_income_taxpayer", 
    "pension_amount", "ration_card", "loan_defaulter", "booth_id", 
    "district", "gali_id", "gali_name", "eligible_schemes", 
    "enrolled_schemes", "gap_schemes", "is_floating_node"
]

schemes = ["Mudra Loan (PMMY)", "Ayushman Bharat", "PM-Kisan", "Ujjwala Yojana"]

with open('voters_data.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    
    for i in range(1, 501):
        epic = f"CPT{str(i).zfill(7)}"
        income = random.randint(5000, 45000)
        eligible = random.sample(schemes, 2)
        enrolled = [eligible[0]]
        gap = [eligible[1]]
        
        writer.writerow([
            epic, f"Voter_{i}", random.randint(18, 85), 
            random.choice(["Male", "Female"]), f"9{random.randint(100000000, 999999999)}",
            random.choice(["General", "OBC", "SC", "ST"]), income,
            round(random.uniform(0, 5), 1), random.choice(["Labour", "Farmer", "Business", "Student"]),
            random.choice([True, False]), random.choice([True, False]), True, 
            random.choice([True, False]), income > 25000, 
            4000 if income < 10000 else 0, True, False, 
            f"B{random.randint(1, 10):03}", "Raichur", f"G{i:03}", 
            "Pushpavathi Nagar", "|".join(eligible), "|".join(enrolled), "|".join(gap),
            random.choice([True, False])
        ])

print("File 'voters_data.csv' created with 500 entries.")
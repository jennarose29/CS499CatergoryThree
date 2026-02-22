from animal_shelter import AnimalShelter

db = AnimalShelter()

print(db.get_statistics_by_rescue_type("Water Rescue"))
print(db.get_age_distribution())
print(db.explain_query({"rescue_type": "Water Rescue"}))
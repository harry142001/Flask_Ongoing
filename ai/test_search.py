from curbside_ai_features import sync_embeddings, search_properties

print("Syncing embedding for the 1 property that has a description...")
sync_embeddings(limit=5)

print()
print("Now searching for: 'apartment in Mississauga'")
results = search_properties("apartment in Mississauga", top_k=5)

for r in results:
    print("-", r.get("address"), "|", r.get("price"))
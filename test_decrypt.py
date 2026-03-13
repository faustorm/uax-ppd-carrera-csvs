# Let's search for names that start with 'Fausto' when decrypted
# decrypt: c -> chr((ord(c) - shift - base) % 26 + base)
# We also need to find any name containing Fausto or similar

def decrypt_name(enc, shift=3):
    alph_l = 'abcdefghijklmnopqrstuvwxyz'
    alph_u = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    result = []
    for c in enc:
        if c in alph_l:
            result.append(alph_l[(alph_l.index(c) - shift) % 26])
        elif c in alph_u:
            result.append(alph_u[(alph_u.index(c) - shift) % 26])
        else:
            result.append(c)
    return "".join(result)

# What would Fausto look like as ciphertext if plain 'Fausto' encrypted?
# F -> I, a -> d, u -> x, s -> v, t -> w, o -> r
# So "Fausto" -> "Idxvwr"
# But if we used accent_map: a doesn't have an accent, so 'a'->'d', and nothing else changes
# Let's be explicit:
# F -> (F is uppercase, index 5), 5+3=8 -> I  ✓
# a -> (lowercase, index 0), 0+3=3 -> d  ✓
# u -> index 20, 20+3=23 -> x  ✓
# s -> index 18, 18+3=21 -> v  ✓
# t -> index 19, 19+3=22 -> w  ✓
# o -> index 14, 14+3=17 -> r  ✓
# So Fausto -> "Idxvwr"

alph_l = 'abcdefghijklmnopqrstuvwxyz'
alph_u = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
name = "Fausto"
shift = 3
enc = ""
for c in name:
    if c in alph_l:
        enc += alph_l[(alph_l.index(c) + shift) % 26]
    elif c in alph_u:
        enc += alph_u[(alph_u.index(c) + shift) % 26]
    else:
        enc += c

print(f"Correct encrypted Fausto (no accent_map for regular chars): '{enc}'")
print(f"My previous encrypt_name with accent_map gave: 'Ibavwt'")
print(f"Difference: accent_map was wrongly catching ASCII letters via overlapping keys!")
print()
print("The problem was that the Python dict definition with duplicate keys kept the LAST value.")
print("e.g. both 'a': 'y' (acuted) and 'a': 'a' (normal) were in the code - the latter overwrote.")

# Now search for this correct pattern
correct_enc = enc
count = 0
print(f"\\nSearching for '{correct_enc}' in the CSV...")
with open('c:/Users/Fausto UAX/code/uax-ppd-carrera-csvs/datos_valientes.csv', 'r', encoding='utf-8') as f:
    next(f)  # skip header
    for i, line in enumerate(f):
        parts = line.split(',')
        if len(parts) >= 2 and correct_enc in parts[1]:
            print(f"  Row {i+1}: '{parts[1]}' -> '{decrypt_name(parts[1])}'")
            count += 1
            if count >= 20:
                print("  ... (stopping at 20)")
                break

print(f"Found {count} rows with '{correct_enc}'")

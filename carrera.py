import os
import multiprocessing
import time
from collections import defaultdict


def encrypt_name(name, shift=3):
    '''Encrypt a plaintext name using the custom Caesar cipher found in the dataset.
    
    The dataset encoder first replaced Spanish accented chars with their unaccented
    equivalents, then applied a Caesar +3 shift.
    
    Accent map (discovered empirically):
        a->y, e->g, i->k, o->q, u->x, n->o  (and uppercase versions)
    '''
    # Using explicit unicode codepoints (\u00e1 etc.) to avoid Python dict duplicate-key bug
    accent_map = {
        '\u00e1': 'y', '\u00e9': 'g', '\u00ed': 'k',
        '\u00f3': 'q', '\u00fa': 'x', '\u00f1': 'o',
        '\u00c1': 'Y', '\u00c9': 'G', '\u00cd': 'K',
        '\u00d3': 'Q', '\u00da': 'X', '\u00d1': 'O',
    }
    alph_l = 'abcdefghijklmnopqrstuvwxyz'
    alph_u = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

    step1 = "".join(accent_map.get(c, c) for c in name)

    result = []
    for c in step1:
        if c in alph_l:
            result.append(alph_l[(alph_l.index(c) + shift) % 26])
        elif c in alph_u:
            result.append(alph_u[(alph_u.index(c) + shift) % 26])
        else:
            result.append(c)
    return "".join(result)


def process_chunk(filename, start_offset, end_offset, tocayos_encrypted, fausto_encrypted, tocayos_raw):
    age_range_salary = {}
    votes_party = {}
    city_salary = {}

    total_salary_valid = 0
    count_salary_valid = 0
    total_age = 0
    count_age = 0
    count_fausto = 0
    count_tocayos = {}  # name -> count
    count_unemployed = 0

    with open(filename, 'r', encoding='utf-8') as f:
        f.seek(start_offset)

        while True:
            line = f.readline()
            if not line:
                break

            # Skip header
            if start_offset == 0 and line.startswith('id,'):
                if f.tell() >= end_offset:
                    break
                continue

            parts = line.split(',')
            if len(parts) == 6:
                nombre = parts[1]
                salario_str = parts[3]
                ciudad = parts[4]
                partido = parts[5].strip()

                try:
                    edad = int(parts[2])
                    total_age += edad
                    count_age += 1
                    edad_range = (edad // 5) * 5
                except ValueError:
                    edad_range = None

                if salario_str == 'PARADO':
                    count_unemployed += 1
                else:
                    try:
                        salario = int(salario_str)
                        total_salary_valid += salario
                        count_salary_valid += 1

                        if edad_range is not None:
                            if edad_range not in age_range_salary:
                                age_range_salary[edad_range] = [0, 0]
                            age_range_salary[edad_range][0] += salario
                            age_range_salary[edad_range][1] += 1

                        if ciudad not in city_salary:
                            city_salary[ciudad] = [0, 0]
                        city_salary[ciudad][0] += salario
                        city_salary[ciudad][1] += 1

                    except ValueError:
                        pass

                votes_party[partido] = votes_party.get(partido, 0) + 1

                if fausto_encrypted in nombre:
                    count_fausto += 1

                for t, raw in zip(tocayos_encrypted, tocayos_raw):
                    if t in nombre:
                        count_tocayos[raw] = count_tocayos.get(raw, 0) + 1

            if f.tell() >= end_offset:
                break

    return {
        'total_salary_valid': total_salary_valid,
        'count_salary_valid': count_salary_valid,
        'total_age': total_age,
        'count_age': count_age,
        'age_range_salary': age_range_salary,
        'votes_party': votes_party,
        'city_salary': city_salary,
        'count_fausto': count_fausto,
        'count_tocayos': count_tocayos,  # dict name->count
        'count_unemployed': count_unemployed
    }


def worker_main(file_path, start_offset, end_offset, pipe_conn, tocayos_encrypted, fausto_encrypted, tocayos_raw):
    stats = process_chunk(file_path, start_offset, end_offset, tocayos_encrypted, fausto_encrypted, tocayos_raw)
    pipe_conn.send(stats)
    pipe_conn.close()


def main():
    file_path = 'c:/Users/Fausto UAX/code/uax-ppd-carrera-csvs/datos_valientes.csv'

    # <<< Replace with your group's first names (no accents needed, just plain names) >>>
    tocayos_raw = ["Fausto", "Rodrigo", "Adriana", "David", "Carlos", "Paula", "Rafael", "Adriana", "Alejandra", "Lucía", "Eva"]
    tocayos_encrypted = [encrypt_name(n) for n in tocayos_raw]
    fausto_encrypted = encrypt_name("Fausto")

    print(f"Searching for 'Fausto' as encrypted: '{fausto_encrypted}'")
    print(f"Searching for tocayos: {list(zip(tocayos_raw, tocayos_encrypted))}")

    num_cores = multiprocessing.cpu_count()
    print(f"Using {num_cores} CPU cores.")

    file_size = os.path.getsize(file_path)
    chunk_size = file_size // num_cores

    # Calculate byte offsets aligned to newlines
    file_offsets = []
    with open(file_path, 'rb') as f:
        current_offset = 0
        for i in range(num_cores):
            start = current_offset
            if i == num_cores - 1:
                end = file_size
            else:
                f.seek(start + chunk_size)
                # Walk forward until we hit a newline
                while True:
                    char = f.read(1)
                    if not char or char == b'\n':
                        break
                end = f.tell()
            file_offsets.append((start, end))
            current_offset = end

    print(f"Chunks: {[(s, e) for s, e in file_offsets]}")
    print("Spawning processes...")

    start_time = time.time()

    # Spawn workers. IMPORTANT: collect results via recv() BEFORE joining
    # to avoid blocking on a full OS pipe buffer (deadlock).
    pipes = []
    processes = []

    for start, end in file_offsets:
        parent_conn, child_conn = multiprocessing.Pipe(duplex=False)
        pipes.append(parent_conn)
        p = multiprocessing.Process(
            target=worker_main,
            args=(file_path, start, end, child_conn, tocayos_encrypted, fausto_encrypted, tocayos_raw)
        )
        processes.append(p)
        p.start()
        child_conn.close()  # Close child end in parent so recv() knows when child is done

    # Receive data FIRST, then join
    results = [conn.recv() for conn in pipes]
    for p in processes:
        p.join()

    read_time = time.time()
    print(f"All processes done in {read_time - start_time:.2f}s. Merging results...")

    # Merge results
    total_salary_valid = 0
    count_salary_valid = 0
    total_age = 0
    count_age = 0
    age_range_salary = {}
    votes_party = {}
    city_salary = {}
    count_fausto = 0
    count_tocayos = {}  # name -> count
    count_unemployed = 0

    for r in results:
        total_salary_valid += r['total_salary_valid']
        count_salary_valid += r['count_salary_valid']
        total_age += r['total_age']
        count_age += r['count_age']
        count_fausto += r['count_fausto']
        for name, cnt in r['count_tocayos'].items():
            count_tocayos[name] = count_tocayos.get(name, 0) + cnt
        count_unemployed += r['count_unemployed']

        for arange, val in r['age_range_salary'].items():
            if arange not in age_range_salary:
                age_range_salary[arange] = [0, 0]
            age_range_salary[arange][0] += val[0]
            age_range_salary[arange][1] += val[1]

        for party, votes in r['votes_party'].items():
            votes_party[party] = votes_party.get(party, 0) + votes

        for city, val in r['city_salary'].items():
            if city not in city_salary:
                city_salary[city] = [0, 0]
            city_salary[city][0] += val[0]
            city_salary[city][1] += val[1]

    # Final computations
    avg_salary = total_salary_valid / count_salary_valid if count_salary_valid > 0 else 0
    avg_age = total_age / count_age if count_age > 0 else 0

    # Age range with highest avg salary
    best_range = max(age_range_salary, key=lambda r: age_range_salary[r][0] / age_range_salary[r][1] if age_range_salary[r][1] > 0 else 0)
    best_range_avg = age_range_salary[best_range][0] / age_range_salary[best_range][1]

    # Winning party coalition (minimum parties to exceed 50% of all votes)
    total_votes = sum(votes_party.values())
    sorted_parties = sorted(votes_party.items(), key=lambda x: x[1], reverse=True)

    def compute_coalition(exclude_pairs=None):
        """Returns (combo_labels, accumulated, is_valid) picking parties in order,
        skipping combinations that contain all parties in any of the excluded pairs."""
        combo = []
        acc = 0
        excluded = set()
        if exclude_pairs:
            for pair in exclude_pairs:
                if all(p in [x[0] for x in sorted_parties[:2]] for p in pair):
                    excluded.update(pair)

        for party, votes in sorted_parties:
            if party in excluded:
                continue
            combo.append(f"{party} ({votes:,})")
            acc += votes
            if acc > total_votes / 2:
                break
        return combo, acc

    winning_combo, accumulated = compute_coalition()
    winning_party_names = [c.split(' (')[0] for c in winning_combo]

    # Check if it's the 'impossible' PP + PSOE scenario
    pp_psoe_impossible = 'PP' in winning_party_names and 'PSOE' in winning_party_names
    alternative_combo, alt_accumulated = None, 0
    if pp_psoe_impossible:
        # Try all combinations of parties (in ranked order) excluding PP or PSOE from the pair
        # Strategy: try without PP, then without PSOE, pick whichever needs fewer parties
        def coalition_excluding(banned):
            combo = []
            acc = 0
            for party, votes in sorted_parties:
                if party == banned:
                    continue
                combo.append(f"{party} ({votes:,})")
                acc += votes
                if acc > total_votes / 2:
                    break
            return combo, acc

        combo_no_pp, acc_no_pp = coalition_excluding('PP')
        combo_no_psoe, acc_no_psoe = coalition_excluding('PSOE')
        # Pick the option with fewer parties (or larger margin)
        if len(combo_no_pp) <= len(combo_no_psoe):
            alternative_combo, alt_accumulated = combo_no_pp, acc_no_pp
        else:
            alternative_combo, alt_accumulated = combo_no_psoe, acc_no_psoe

    # City ranking by average salary (only cities with >100 salaried people for stability)
    city_avg = [(city, val[0] / val[1]) for city, val in city_salary.items() if val[1] > 100]
    city_avg.sort(key=lambda x: x[1], reverse=True)
    top_3_rich = city_avg[:3]
    top_3_poor = city_avg[-3:]

    total_time = time.time() - start_time

    print()
    print("=" * 50)
    print(f"  RESULTADOS - Tiempo total: {total_time:.2f}s")
    print("=" * 50)
    print(f"1. Salario medio:          {avg_salary:,.2f} EUR")
    print(f"2. Edad media:             {avg_age:.2f} anos")
    print(f"3. Rango de edad (5 anos) con mayor salario:")
    print(f"   {best_range} - {best_range+4} anos  (avg: {best_range_avg:,.2f} EUR)")
    print(f"4. Coalicion ganadora (>50% votos):")
    if pp_psoe_impossible:
        print(f"   Matematicamente: {' + '.join(winning_combo)}")
        print(f"   ({accumulated:,} / {total_votes:,} = {accumulated/total_votes*100:.2f}%)")
        print(f"   *** PP y PSOE juntos es IMPOSIBLE en la practica ***")
        print(f"   Siguiente mejor coalicion viable:")
        print(f"   {' + '.join(alternative_combo)}")
        print(f"   ({alt_accumulated:,} / {total_votes:,} = {alt_accumulated/total_votes*100:.2f}%)")
    else:
        print(f"   {' + '.join(winning_combo)}")
        print(f"   ({accumulated:,} / {total_votes:,} votos = {accumulated/total_votes*100:.2f}%)")
    print(f"5. Personas llamadas 'Fausto': {count_fausto}")
    total_tocayos = sum(count_tocayos.values())
    print(f"6. Tocayos del grupo - Total: {total_tocayos}")
    # Show unique names (deduplicated) with their counts
    seen = {}
    for name in tocayos_raw:
        if name not in seen:
            seen[name] = count_tocayos.get(name, 0)
    for name, cnt in sorted(seen.items(), key=lambda x: x[1], reverse=True):
        print(f"   - {name}: {cnt}")
    print(f"7. Ciudades mas ricas (avg salario):")
    for city, s in top_3_rich:
        print(f"   - {city}: {s:,.2f}")
    print(f"   Ciudades mas pobres:")
    for city, s in reversed(top_3_poor):
        print(f"   - {city}: {s:,.2f}")
    print(f"8. Numero de parados: {count_unemployed:,}")
    print("=" * 50)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()

#!/usr/bin/env python3
"""
DK UFC CSV → slate-mma.json converter
Run: python convert_ufc.py DKSalaries.csv
Outputs: slate-mma.json with fighters, fights auto-detected, odds fields blank to fill in.
"""
import csv
import json
import sys
from datetime import datetime

def parse_dk_csv(filepath):
    fighters = []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        content = f.read()
    for line in content.strip().split('\n'):
        parts = line.split(',')
        if len(parts) >= 16:
            try:
                name = parts[9].strip()
                pid = parts[10].strip()
                salary = parts[12].strip()
                game_info = parts[13].strip()
                team = parts[14].strip()
                avg_ppg = parts[15].strip()
                if name and pid.isdigit() and salary.isdigit() and int(salary) > 0:
                    # Skip cancelled fights
                    if 'Cancelled' in game_info:
                        print(f"  ⚠️  Skipping {name} (cancelled)")
                        continue
                    fighters.append({
                        'name': name,
                        'id': int(pid),
                        'salary': int(salary),
                        'game_info': game_info,
                        'team': team,
                        'avg_ppg': float(avg_ppg) if avg_ppg else 0
                    })
            except (ValueError, IndexError):
                continue
    return fighters

def detect_fights(fighters):
    """Group fighters into fights using game_info field."""
    fight_map = {}
    for f in fighters:
        # Game info: "TeamA@TeamB MM/DD/YYYY HH:MMPM ET"
        key = f['game_info'].split(' ')[0] if f['game_info'] else ''
        if not key:
            continue
        if key not in fight_map:
            fight_map[key] = []
        fight_map[key].append(f)
    fights = []
    for key, flist in fight_map.items():
        if len(flist) == 2:
            time_str = ''
            try:
                parts = flist[0]['game_info'].split(' ')
                if len(parts) >= 3:
                    time_str = f"{parts[1]} {parts[2]} ET"
            except Exception:
                pass
            # Sort so higher-salary fighter is A (conventional)
            a, b = sorted(flist, key=lambda x: -x['salary'])
            fights.append({
                'fighter_a': a['name'],
                'fighter_b': b['name'],
                'start_time': time_str,
                'rounds': 3,  # Manually change to 5 for main events
                'odds': {
                    'ml_a': -200, 'ml_b': 165,
                    'method_a_ko': 500, 'method_a_sub': 800, 'method_a_dec': 200,
                    'method_b_ko': 600, 'method_b_sub': 1500, 'method_b_dec': 400,
                    'method_draw': 8000,
                    'a_r1': 600, 'a_r2': 900, 'a_r3': 1400, 'a_points': 200,
                    'b_r1': 800, 'b_r2': 1200, 'b_r3': 1800, 'b_points': 400,
                    'total_rounds_line': 2.5, 'total_rounds_over': -140, 'total_rounds_under': 110
                },
                'ct_a': 2.0,
                'ct_b': 1.5,
                'ss_line_a': 45.0,
                'ss_line_b': 45.0,
                'adj_a': 0,
                'adj_b': 0,
                'notes': ''
            })
    return fights

def main():
    if len(sys.argv) < 2:
        print("Usage: python convert_ufc.py DKSalaries.csv [output.json]")
        sys.exit(1)
    filepath = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) > 2 else 'slate-mma.json'
    fighters = parse_dk_csv(filepath)
    fights = detect_fights(fighters)
    today = datetime.now().strftime('%Y-%m-%d')
    slate = {
        'sport': 'mma',
        'date': today,
        'event': 'UFC Fight Night',
        'contest_size': 0,
        'salary_cap': 50000,
        'roster_size': 6,
        'fights': fights,
        'dk_players': [
            {'name': f['name'], 'id': f['id'], 'salary': f['salary'], 'avg_ppg': f['avg_ppg']}
            for f in fighters
        ],
        'pp_lines': [],
        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M %p MT')
    }
    with open(output, 'w') as fp:
        json.dump(slate, fp, indent=2)
    print(f"\n✅ Created {output}")
    print(f"   {len(fighters)} fighters")
    print(f"   {len(fights)} fights")
    print(f"\nNext steps:")
    print(f"  1. Open {output} in a text editor")
    print(f"  2. Set 'rounds': 5 for the main event (default is 3)")
    print(f"  3. Replace default odds with bet365 odds for each fight")
    print(f"  4. Set ct_a / ct_b from PP control-time lines")
    print(f"  5. Set ss_line_a / ss_line_b from Underdog/PP sig-strike lines")
    print(f"  6. Fill pp_lines with fighter/stat/line/mult")
    print(f"  7. Set 'event' name and 'contest_size' (DK entries)")
    print(f"  8. Upload to GitHub → Vercel auto-deploys")

if __name__ == '__main__':
    main()

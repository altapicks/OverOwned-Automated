#!/usr/bin/env python3
"""
DK CSV → slate.json converter
Run: python convert.py DKSalaries.csv
Outputs: slate.json with all players, matches auto-detected, odds fields ready to fill in.
"""
import csv
import json
import sys
import re
from datetime import datetime

def parse_dk_csv(filepath):
    players = []
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
                    players.append({
                        'name': name,
                        'id': int(pid),
                        'salary': int(salary),
                        'game_info': game_info,
                        'team': team,
                        'avg_ppg': float(avg_ppg) if avg_ppg else 0
                    })
            except (ValueError, IndexError):
                continue
    return players

def detect_matches(players):
    """Group players into matches using game_info field."""
    match_map = {}
    for p in players:
        # Game info format: "TeamA@TeamB MM/DD/YYYY HH:MMAM ET"
        key = p['game_info'].split(' ')[0] if p['game_info'] else ''
        if key not in match_map:
            match_map[key] = []
        match_map[key].append(p)
    
    matches = []
    for key, plist in match_map.items():
        if len(plist) == 2:
            # Parse start time from game_info
            time_str = ''
            try:
                parts = plist[0]['game_info'].split(' ')
                if len(parts) >= 3:
                    date_str = parts[1]  # MM/DD/YYYY
                    time_part = parts[2]  # HH:MMAM
                    time_str = f"{date_str} {time_part}"
            except:
                pass
            
            matches.append({
                'player_a': plist[0]['name'],
                'player_b': plist[1]['name'],
                'start_time': time_str,
                'tournament': '',  # Fill in manually
                'odds': {
                    'ml_a': -200, 'ml_b': 160,
                    'set_a_20': -120, 'set_a_21': 240,
                    'set_b_20': 600, 'set_b_21': 550,
                    'gw_a_line': 12.5, 'gw_a_over': -120,
                    'gw_b_line': 10.5, 'gw_b_over': -120,
                    'brk_a_line': 2.5, 'brk_a_over': -150,
                    'brk_b_line': 1.5, 'brk_b_over': -120,
                    'ace_a_5plus': -150, 'ace_a_10plus': 500,
                    'ace_b_5plus': -150, 'ace_b_10plus': 500,
                    'df_a_2plus': -200, 'df_a_3plus': 150,
                    'df_b_2plus': -200, 'df_b_3plus': 150
                },
                'adj_a': 0,
                'adj_b': 0
            })
    return matches

def main():
    if len(sys.argv) < 2:
        print("Usage: python convert.py DKSalaries.csv")
        print("       python convert.py DKSalaries.csv output.json")
        sys.exit(1)
    
    filepath = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) > 2 else 'slate.json'
    
    players = parse_dk_csv(filepath)
    matches = detect_matches(players)
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    slate = {
        'date': today,
        'matches': matches,
        'dk_players': [
            {'name': p['name'], 'id': p['id'], 'salary': p['salary'], 'avg_ppg': p['avg_ppg']}
            for p in players
        ],
        'pp_lines': []  # Fill in manually
    }
    
    with open(output, 'w') as f:
        json.dump(slate, f, indent=2)
    
    print(f"Created {output}")
    print(f"  {len(players)} players")
    print(f"  {len(matches)} matches")
    print(f"\nNext steps:")
    print(f"  1. Open {output} in a text editor")
    print(f"  2. Fill in tournament names")
    print(f"  3. Replace default odds with bet365 odds for each match")
    print(f"  4. Add PP lines if needed")
    print(f"  5. Add your adjustments (adj_a, adj_b)")
    print(f"  6. Upload to GitHub → Vercel auto-deploys")

if __name__ == '__main__':
    main()

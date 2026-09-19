// AbuseIPDB API 테스트 명령어 및 결과 예시

```
// Case 1 — 118.25.6.39
curl -G https://api.abuseipdb.com/api/v2/check \
  --data-urlencode "ipAddress=118.25.6.39" \
  -d "maxAgeInDays=90" \
  -H "Key: ${API_KEY}" \
  -H "Accept: application/json"

// Case 2 — 144.48.243.18

// Case 2 — 255.255.255.999
```

```json
// Case 1 — 118.25.6.39: abuseConfidenceScore=0, totalReports=1
{
  "data": {
    "ipAddress": "118.25.6.39",
    "isPublic": true,
    "ipVersion": 4,
    "isWhitelisted": false,
    "abuseConfidenceScore": 0,
    "countryCode": "CN",
    "usageType": "Data Center\/Web Hosting\/Transit",
    "isp": "Tencent Cloud Computing (Beijing) Co., Ltd",
    "domain": "tencentcloud.com",
    "hostnames": [
      
    ],
    "isTor": false,
    "totalReports": 1,
    "numDistinctUsers": 1,
    "lastReportedAt": "2026-08-31T18:08:32+00:00"
  }
}

// Case 2 — 144.48.243.18: abuseConfidenceScore=100, totalReports=3319
{
  "data": {
    "ipAddress": "144.48.243.18",
    "isPublic": true,
    "ipVersion": 4,
    "isWhitelisted": false,
    "abuseConfidenceScore": 100,
    "countryCode": "HK",
    "usageType": "Data Center\/Web Hosting\/Transit",
    "isp": "Law's Cloud Infrastructure Limited",
    "domain": "cloudie.hk",
    "hostnames": [
      
    ],
    "isTor": false,
    "totalReports": 3319,
    "numDistinctUsers": 795,
    "lastReportedAt": "2026-09-16T18:57:24+00:00"
  }
}

// Case 3 — 255.255.255.999: invalid IP address, HTTP 422
{
  "errors": [
    {
      "detail": "The ip address must be a valid IPv4 or IPv6 address (e.g. 8.8.8.8 or 2001:4860:4860::8888).",
      "status": 422,
      "source": {
        "parameter": "ipAddress"
      }
    }
  ]
}

// Reference — 192.0.2.1: Reserved address
{
  "data": {
    "ipAddress": "192.0.2.1",
    "isPublic": false,
    "ipVersion": 4,
    "isWhitelisted": true,
    "abuseConfidenceScore": 0,
    "countryCode": null,
    "usageType": "Reserved",
    "isp": null,
    "domain": null,
    "hostnames": [
      
    ],
    "isTor": false,
    "totalReports": 113,
    "numDistinctUsers": 28,
    "lastReportedAt": "2026-09-16T09:47:56+00:00"
  }
}

// Reference — 255.255.255.123: Reserved address
{
  "data": {
    "ipAddress": "255.255.255.123",
    "isPublic": false,
    "ipVersion": 4,
    "isWhitelisted": false,
    "abuseConfidenceScore": 0,
    "countryCode": null,
    "usageType": "Reserved",
    "isp": null,
    "domain": null,
    "hostnames": [
      
    ],
    "isTor": false,
    "totalReports": 0,
    "numDistinctUsers": 0,
    "lastReportedAt": null
  }
}
```

// VirusTotal API 테스트 명령 및 결과 예시
```
// Case 1 — 275a021b...: EICAR test file
curl --request GET \
  --url https://www.virustotal.com/api/v3/files/275a021bbfb6489e54d471899f7db9d1663fc695ec2fe2a2c4538aabf651fd0f \
  --header "x-apikey: ${API_KEY}"

// Case 2 — 2546dc...: malicious detections + Sigma high/medium matches
2546dcffc5ad854d4ddc64fbf056871cd5a00f2471cb7a5bfd4ac23b6e9eedad

// Case 3 - 000000...0001: file not found
0000000000000000000000000000000000000000000000000000000000000001
```

```json
// Case 1 — 275a021b...: EICAR test file
{
  "data": {
    "id": "275a021bbfb6489e54d471899f7db9d1663fc695ec2fe2a2c4538aabf651fd0f",
    "type": "file",
    "links": {
      "self": "https://www.virustotal.com/api/v3/files/275a021bbfb6489e54d471899f7db9d1663fc695ec2fe2a2c4538aabf651fd0f"
    },
    "attributes": {
      "last_analysis_results": {
        "Lionic": {
          "method": "blacklist",
          "engine_name": "Lionic",
          "engine_version": "8.16",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Test.File.EICAR.y"
        },
        "Elastic": {
          "method": "blacklist",
          "engine_name": "Elastic",
          "engine_version": "4.0.288",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "eicar"
        },
        "MicroWorld-eScan": {
          "method": "blacklist",
          "engine_name": "MicroWorld-eScan",
          "engine_version": "14.0.409.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "ClamAV": {
          "method": "blacklist",
          "engine_name": "ClamAV",
          "engine_version": "1.5.4.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar-Test-Signature"
        },
        "CTX": {
          "method": "blacklist",
          "engine_name": "CTX",
          "engine_version": "2024.8.29.1",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "txt.virus.eicar"
        },
        "CAT-QuickHeal": {
          "method": "blacklist",
          "engine_name": "CAT-QuickHeal",
          "engine_version": "22.00",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR.TestFile"
        },
        "Skyhigh": {
          "method": "blacklist",
          "engine_name": "Skyhigh",
          "engine_version": "v2021.2.0+4045",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR test file"
        },
        "ALYac": {
          "method": "blacklist",
          "engine_name": "ALYac",
          "engine_version": "2.0.0.10",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Misc.Eicar-Test-File"
        },
        "Malwarebytes": {
          "method": "blacklist",
          "engine_name": "Malwarebytes",
          "engine_version": "3.1.0.272",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-AV-Test"
        },
        "Zillya": {
          "method": "blacklist",
          "engine_name": "Zillya",
          "engine_version": "2.0.0.5691",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR.TestFile"
        },
        "Sangfor": {
          "method": "blacklist",
          "engine_name": "Sangfor",
          "engine_version": "2.22.3.0",
          "engine_update": "20260915",
          "category": "malicious",
          "result": "EICAR-Test-File (not a virus)"
        },
        "K7AntiVirus": {
          "method": "blacklist",
          "engine_name": "K7AntiVirus",
          "engine_version": "14.79.60818",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_Test_File"
        },
        "BitDefender": {
          "method": "blacklist",
          "engine_name": "BitDefender",
          "engine_version": "7.2",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File (not a virus)"
        },
        "K7GW": {
          "method": "blacklist",
          "engine_name": "K7GW",
          "engine_version": "14.79.60821",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_Test_File"
        },
        "CrowdStrike": {
          "method": "blacklist",
          "engine_name": "CrowdStrike",
          "engine_version": "1.0",
          "engine_update": "20251219",
          "category": "undetected",
          "result": null
        },
        "huorong": {
          "method": "blacklist",
          "engine_name": "huorong",
          "engine_version": "8da36db:8da36db:d202e75:d202e75",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "TEST/AVEngTestFile!EICAR"
        },
        "VirIT": {
          "method": "blacklist",
          "engine_name": "VirIT",
          "engine_version": "9.5.1295",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "SymantecMobileInsight": {
          "method": "blacklist",
          "engine_name": "SymantecMobileInsight",
          "engine_version": "2.0",
          "engine_update": "20260123",
          "category": "malicious",
          "result": "ALG:EICAR Test String"
        },
        "Symantec": {
          "method": "blacklist",
          "engine_name": "Symantec",
          "engine_version": "1.22.0.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR Test String"
        },
        "ESET-NOD32": {
          "method": "blacklist",
          "engine_name": "ESET-NOD32",
          "engine_version": "18.2.18.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar test file"
        },
        "Zoner": {
          "method": "blacklist",
          "engine_name": "Zoner",
          "engine_version": "2.2.2.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR.Test.File-NoVirus.250"
        },
        "TrendMicro-HouseCall": {
          "method": "blacklist",
          "engine_name": "TrendMicro-HouseCall",
          "engine_version": "24.550.0.1002",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar_test_file"
        },
        "Cynet": {
          "method": "blacklist",
          "engine_name": "Cynet",
          "engine_version": "4.0.3.4",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Malicious (score: 99)"
        },
        "Kaspersky": {
          "method": "blacklist",
          "engine_name": "Kaspersky",
          "engine_version": "22.0.1.28",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "Alibaba": {
          "method": "blacklist",
          "engine_name": "Alibaba",
          "engine_version": "0.3.0.5",
          "engine_update": "20190527",
          "category": "malicious",
          "result": "Virus:Win32/EICAR.A"
        },
        "NANO-Antivirus": {
          "method": "blacklist",
          "engine_name": "NANO-Antivirus",
          "engine_version": "1.0.170.26895",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Marker.Dos.EICAR-Test-File.dyb"
        },
        "ViRobot": {
          "method": "blacklist",
          "engine_name": "ViRobot",
          "engine_version": "2014.3.20.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-test"
        },
        "Tencent": {
          "method": "blacklist",
          "engine_name": "Tencent",
          "engine_version": "1.0.0.1",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR.TEST.NOT-A-VIRUS"
        },
        "Sophos": {
          "method": "blacklist",
          "engine_name": "Sophos",
          "engine_version": "3.6.2.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-AV-Test"
        },
        "F-Secure": {
          "method": "blacklist",
          "engine_name": "F-Secure",
          "engine_version": "18.10.1547.307",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_Test_File"
        },
        "VIPRE": {
          "method": "blacklist",
          "engine_name": "VIPRE",
          "engine_version": "6.0.0.35",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File (not a virus)"
        },
        "TrendMicro": {
          "method": "blacklist",
          "engine_name": "TrendMicro",
          "engine_version": "24.550.0.1002",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar_test_file"
        },
        "McAfeeD": {
          "method": "blacklist",
          "engine_name": "McAfeeD",
          "engine_version": "1.2.0.16123",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "WPS!EICAR"
        },
        "CMC": {
          "method": "blacklist",
          "engine_name": "CMC",
          "engine_version": "2.4.2022.1",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar.test.file"
        },
        "Emsisoft": {
          "method": "blacklist",
          "engine_name": "Emsisoft",
          "engine_version": "2024.8.0.61147",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File (A)"
        },
        "Ikarus": {
          "method": "blacklist",
          "engine_name": "Ikarus",
          "engine_version": "6.5.4.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "GData": {
          "method": "blacklist",
          "engine_name": "GData",
          "engine_version": "GD:27.46018AVA:64.31910",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_TEST_FILE"
        },
        "Jiangmin": {
          "method": "blacklist",
          "engine_name": "Jiangmin",
          "engine_version": "16.0.100",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "Webroot": {
          "method": "blacklist",
          "engine_name": "Webroot",
          "engine_version": "1.9.0.8",
          "engine_update": "20250227",
          "category": "malicious",
          "result": "W32.Eicar.Testvirus.Gen"
        },
        "Varist": {
          "method": "blacklist",
          "engine_name": "Varist",
          "engine_version": "6.6.1.3",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_Test_File"
        },
        "Avira": {
          "method": "blacklist",
          "engine_name": "Avira",
          "engine_version": "8.3.3.24",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar-Test-Signature"
        },
        "Antiy-AVL": {
          "method": "blacklist",
          "engine_name": "Antiy-AVL",
          "engine_version": "3.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "TestFile/Win32.EICAR"
        },
        "Kingsoft": {
          "method": "blacklist",
          "engine_name": "Kingsoft",
          "engine_version": "None",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Test.eicar.aa"
        },
        "Microsoft": {
          "method": "blacklist",
          "engine_name": "Microsoft",
          "engine_version": "1.26080",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Virus:DOS/EICAR_Test_File"
        },
        "Gridinsoft": {
          "method": "blacklist",
          "engine_name": "Gridinsoft",
          "engine_version": "1.0.254.174",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Trojan.U.EICAR_Test_File.dd"
        },
        "Xcitium": {
          "method": "blacklist",
          "engine_name": "Xcitium",
          "engine_version": "38963",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Malware@#2975xfk8s2pq1"
        },
        "Arcabit": {
          "method": "blacklist",
          "engine_name": "Arcabit",
          "engine_version": "2025.0.0.23",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File (not a virus)"
        },
        "SUPERAntiSpyware": {
          "method": "blacklist",
          "engine_name": "SUPERAntiSpyware",
          "engine_version": "5.6.0.1032",
          "engine_update": "20260915",
          "category": "malicious",
          "result": "NotAThreat.EICAR[TestFile]"
        },
        "ZoneAlarm": {
          "method": "blacklist",
          "engine_name": "ZoneAlarm",
          "engine_version": "6.27-118568656",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-AV-Test"
        },
        "Avast-Mobile": {
          "method": "blacklist",
          "engine_name": "Avast-Mobile",
          "engine_version": "260916-00",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar"
        },
        "Google": {
          "method": "blacklist",
          "engine_name": "Google",
          "engine_version": "1789578059",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Detected"
        },
        "AhnLab-V3": {
          "method": "blacklist",
          "engine_name": "AhnLab-V3",
          "engine_version": "3.30.1.10706",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Virus/EICAR_Test_File"
        },
        "Acronis": {
          "method": "blacklist",
          "engine_name": "Acronis",
          "engine_version": "1.2.0.121",
          "engine_update": "20240328",
          "category": "undetected",
          "result": null
        },
        "VBA32": {
          "method": "blacklist",
          "engine_name": "VBA32",
          "engine_version": "5.7.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "TACHYON": {
          "method": "blacklist",
          "engine_name": "TACHYON",
          "engine_version": "2026-09-16.02",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "APEX": {
          "method": "blacklist",
          "engine_name": "APEX",
          "engine_version": "6.821",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR Anti-Virus Test File"
        },
        "Rising": {
          "method": "blacklist",
          "engine_name": "Rising",
          "engine_version": "25.0.0.28",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Virus.EICARTestFile!1.103DB (CLASSIC)"
        },
        "Yandex": {
          "method": "blacklist",
          "engine_name": "Yandex",
          "engine_version": "5.5.2.24",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_test_file"
        },
        "TrellixENS": {
          "method": "blacklist",
          "engine_name": "TrellixENS",
          "engine_version": "6.0.6.653",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR test file"
        },
        "SentinelOne": {
          "method": "blacklist",
          "engine_name": "SentinelOne",
          "engine_version": "7.7.0.1",
          "engine_update": "20260714",
          "category": "malicious",
          "result": "Static AI - Malicious COM"
        },
        "MaxSecure": {
          "method": "blacklist",
          "engine_name": "MaxSecure",
          "engine_version": "1.0.0.1",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "VIRUS.EICAR.TEST"
        },
        "Fortinet": {
          "method": "blacklist",
          "engine_name": "Fortinet",
          "engine_version": "7.0.48.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_TEST_FILE"
        },
        "Panda": {
          "method": "blacklist",
          "engine_name": "Panda",
          "engine_version": "4.6.4.2",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-AV-TEST-FILE"
        },
        "alibabacloud": {
          "method": "blacklist",
          "engine_name": "alibabacloud",
          "engine_version": "2.2.0",
          "engine_update": "20250321",
          "category": "malicious",
          "result": "Engtest:Multi/Eicar"
        },
        "Bkav": {
          "method": "blacklist",
          "engine_name": "Bkav",
          "engine_version": null,
          "engine_update": "20260916",
          "category": "timeout",
          "result": null
        },
        "DrWeb": {
          "method": "blacklist",
          "engine_name": "DrWeb",
          "engine_version": "7.0.76.8280",
          "engine_update": "20260916",
          "category": "timeout",
          "result": null
        },
        "Avast": {
          "method": "blacklist",
          "engine_name": "Avast",
          "engine_version": "23.9.8494.0",
          "engine_update": "20260916",
          "category": "timeout",
          "result": null
        },
        "AVG": {
          "method": "blacklist",
          "engine_name": "AVG",
          "engine_version": "23.9.8494.0",
          "engine_update": "20260916",
          "category": "timeout",
          "result": null
        },
        "BitDefenderFalx": {
          "method": "blacklist",
          "engine_name": "BitDefenderFalx",
          "engine_version": "2.0.936",
          "engine_update": "20260903",
          "category": "type-unsupported",
          "result": null
        },
        "DeepInstinct": {
          "method": "blacklist",
          "engine_name": "DeepInstinct",
          "engine_version": "5.0.0.8",
          "engine_update": "20260614",
          "category": "type-unsupported",
          "result": null
        },
        "tehtris": {
          "method": "blacklist",
          "engine_name": "tehtris",
          "engine_version": "v0.1.4",
          "engine_update": "20260916",
          "category": "type-unsupported",
          "result": null
        },
        "Paloalto": {
          "method": "blacklist",
          "engine_name": "Paloalto",
          "engine_version": "0.9.0.1003",
          "engine_update": "20260916",
          "category": "type-unsupported",
          "result": null
        },
        "Cylance": {
          "method": "blacklist",
          "engine_name": "Cylance",
          "engine_version": "3.0.0.0",
          "engine_update": "20260910",
          "category": "type-unsupported",
          "result": null
        },
        "Trustlook": {
          "method": "blacklist",
          "engine_name": "Trustlook",
          "engine_version": "1.0",
          "engine_update": "20260916",
          "category": "type-unsupported",
          "result": null
        }
      },
      "ssdeep": "3:a+JraNvsgzsVqSwHq9:tJuOgzsko",
      "tlsh": "T141A022003B0EEE2BA20B00200032E8B00808020E2CE00A3820A020B8C83308803EC228",
      "first_seen_itw_date": 1339694174,
      "trid": [
        {
          "file_type": "EICAR antivirus test file",
          "probability": 100.0
        }
      ],
      "crowdsourced_ids_results": [
        {
          "rule_category": "protocol-command-decode",
          "alert_severity": "low",
          "rule_msg": "(tcp) experimental TCP options found",
          "rule_id": "116:58",
          "rule_source": "Snort registered user ruleset",
          "rule_url": "https://www.snort.org/downloads/#rule-downloads",
          "rule_raw": "alert ( gid:116; sid:58; rev:2; msg:\"(tcp) experimental TCP options found\"; metadata: policy max-detect-ips drop, rule-type decode; classtype:protocol-command-decode;)",
          "alert_context": [
            {
              "src_ip": "8.8.8.8",
              "src_port": 53
            }
          ]
        }
      ],
      "last_analysis_date": 1789584677,
      "magic": "EICAR virus test files",
      "sha256": "275a021bbfb6489e54d471899f7db9d1663fc695ec2fe2a2c4538aabf651fd0f",
      "crowdsourced_ids_stats": {
        "high": 0,
        "medium": 0,
        "low": 1,
        "info": 0
      },
      "names": [
        "virustestfile.txt",
        "eicar.com-6709",
        "eicar.com-1989",
        "eicar.com",
        "eicar.com-46299",
        "LOG.txt",
        "eicar.com-42046",
        "eicar.com-37629",
        "eicar.com-33367",
        "eicar.com-28848",
        "eicar.com-24470",
        "eicar.com-20150",
        "eicar.com-15802",
        "eicar.com-11420",
        "eicar.com-7164",
        "eicar.com.txt",
        "eicar.com-2298",
        "eicar.com-46724",
        "eicar.com-42261",
        "eicar.com-37329",
        "eicar.com-32945",
        "secret.txt",
        "eicar_test.txt",
        "eicar.com-28574",
        "eicar.com-23968",
        "eicar-test.html",
        "eicar.com-19596",
        "test-eicar.txt",
        "eicar.com-14738",
        "online_viewer_net(3).html",
        "eicar.com-10046",
        "eicar.com-5774",
        "eicar.com-775",
        "eicor.com.txt",
        "eicar.com-40935",
        "eicar.com-36591",
        "eicar.com.1",
        "eicar.com-32206",
        "virus_test_file.txt",
        "eicar.com-27931",
        "\u041d\u043e\u0432\u0438\u0439 \u0442\u0435\u043a\u0441\u0442\u043e\u0432\u0438\u0439 \u0434\u043e\u043a\u0443\u043c\u0435\u043d\u0442.txt",
        "eicar.com-23505",
        "TestVirustotal.txt",
        "eicar.txt",
        "eicar.com-19206",
        "eicar.com-14828",
        "eicar.com-10336",
        "eicar.com-6056",
        "eicar.com-1210",
        "eicar.com-45627",
        "eicar.com-41173",
        "eicar.com-36932",
        "eicar.com-32490",
        "test.txt",
        "eicar.com-28211",
        "eicar.com-23826",
        "eicar.com-19463",
        "eicar.com-14759",
        "eicar.com-9958",
        "eicar.com-5503",
        "eicar.com-838",
        "eicar.com-45201",
        "eicar(1).com",
        "eicar.com-40880",
        "eicar.com-36501",
        "eicar_test (1).txt",
        "Ny Tekstdokument.txt",
        "test.yml",
        "Test fike.txt",
        "eicar.com-31285",
        "eicar.com-22534",
        "przyk\u0142ad virusa.txt",
        "eicar.com-17853",
        "eicar.com-13458",
        "eicar.com-8011",
        "eicar.com-38757",
        "eicar.com-34497",
        "eicar.com-30021",
        "eicar.com-25726",
        "eicar.com-21402",
        "eicar.com-17035",
        "eicar.com-11020",
        "eicar.com-48333",
        "eicar.com-44032",
        "eicar.com-38969",
        "cc.txt",
        "test wirus.txt",
        "qwertyuiop.txt",
        "wirus.txt",
        "teststowy wirus.txt",
        "eicar.com-34544",
        "eicar.com-30269",
        "eicar.com-25861",
        "eicar_test.com",
        "eicar.com-21562",
        "eicar.com-17170",
        "kikif.txt"
      ],
      "md5": "44d88612fea8a8f36de82e1278abb02f",
      "first_submission_date": 1148301722,
      "sigma_analysis_stats": {
        "high": 0,
        "medium": 0,
        "critical": 0,
        "low": 1
      },
      "size": 68,
      "total_votes": {
        "harmless": 2304,
        "malicious": 419
      },
      "type_extension": "ps1",
      "sha1": "3395856ce81f2b7382dee72602f798b642f14140",
      "type_tag": "powershell",
      "sandbox_verdicts": {
        "Zenbox": {
          "category": "malicious",
          "malware_classification": [
            "MALWARE",
            "TROJAN"
          ],
          "sandbox_name": "Zenbox",
          "malware_names": [
            "EICAR"
          ],
          "confidence": 56
        },
        "Lastline": {
          "category": "malicious",
          "sandbox_name": "Lastline",
          "malware_classification": [
            "MALWARE",
            "TROJAN"
          ]
        },
        "OS X Sandbox": {
          "category": "malicious",
          "confidence": 52,
          "sandbox_name": "OS X Sandbox",
          "malware_classification": [
            "MALWARE",
            "TROJAN",
            "EVADER"
          ],
          "malware_names": [
            "EICAR"
          ]
        },
        "Yomi Hunter": {
          "category": "harmless",
          "malware_classification": [
            "CLEAN"
          ],
          "sandbox_name": "Yomi Hunter"
        }
      },
      "last_modification_date": 1789584886,
      "crowdsourced_yara_results": [
        {
          "ruleset_id": "0019ab4291",
          "rule_name": "malw_eicar",
          "ruleset_name": "MALW_Eicar",
          "description": "Rule to detect the EICAR pattern",
          "author": "Marc Rivero | McAfee ATR Team",
          "match_date": 1789584886,
          "ruleset_version": "0019ab4291|fc51a3fe3b450838614a5a5aa327c6bd8689cbb2",
          "source": "https://github.com/advanced-threat-research/Yara-Rules"
        },
        {
          "ruleset_id": "015dce072d",
          "rule_name": "Multi_EICAR_ac8f42d6",
          "ruleset_name": "Multi_EICAR",
          "author": "Elastic Security",
          "match_date": 1789584886,
          "ruleset_version": "015dce072d|195c9611ddb90db599d7ffc1a9b0e8c45688007d",
          "source": "https://github.com/elastic/protections-artifacts"
        },
        {
          "ruleset_id": "000720c1f3",
          "rule_name": "SUSP_Just_EICAR",
          "ruleset_name": "gen_suspicious_strings",
          "description": "Just an EICAR test file - this is boring but users asked for it",
          "author": "Florian Roth (Nextron Systems)",
          "match_date": 1789584886,
          "ruleset_version": "000720c1f3|fcd62734f98eab177e4970751fd1cc901bc8654e",
          "source": "https://github.com/Neo23x0/signature-base"
        }
      ],
      "magika": "POWERSHELL",
      "type_tags": [
        "source",
        "powershell",
        "ps",
        "ps1"
      ],
      "reputation": 3795,
      "sigma_analysis_summary": {
        "Sigma Integrated Rule Set (GitHub)": {
          "high": 0,
          "medium": 0,
          "critical": 0,
          "low": 1
        }
      },
      "saferpickle": {
        "justification": "Not a pickle file",
        "classification": "unknown"
      },
      "type_description": "Powershell",
      "times_submitted": 1164054,
      "meaningful_name": "virustestfile.txt",
      "unique_sources": 3557,
      "known_distributors": {
        "distributors": [
          "OffSec Services Limited"
        ],
        "filenames": [
          "eicar.com"
        ],
        "products": [
          "BlackArch Linux",
          "Kali Linux for Apple Silicon",
          "Kali Linux",
          "Kali Linux Purple",
          "Kali Linux ISO 64-bit",
          "Kali Linux Nethunter"
        ],
        "data_sources": [
          "National Software Reference Library (NSRL)"
        ]
      },
      "tags": [
        "via-tor",
        "detect-debug-environment",
        "long-sleeps",
        "known-distributor",
        "direct-cpu-clock-access",
        "idle",
        "powershell",
        "attachment"
      ],
      "filecondis": {
        "raw_md5": "bcf2bafa8b4e580d7c0f48b4c698f596",
        "dhash": "9300009100008090"
      },
      "sigma_analysis_results": [
        {
          "match_context": [
            {
              "values": {
                "EventID": "4672",
                "PrivilegeList": "SeAssignPrimaryTokenPrivilege\r\n\t\t\tSeTcbPrivilege\r\n\t\t\tSeSecurityPrivilege\r\n\t\t\tSeTakeOwnershipPrivilege\r\n\t\t\tSeLoadDriverPrivilege\r\n\t\t\tSeBackupPrivilege\r\n\t\t\tSeRestorePrivilege\r\n\t\t\tSeDebugPrivilege\r\n\t\t\tSeAuditPrivilege\r\n\t\t\tSeSystemEnvironmentPrivilege\r\n\t\t\tSeImpersonatePrivilege\r\n\t\t\tSeDelegateSessionUserImpersonatePrivilege",
                "SubjectUserName": "SYSTEM",
                "SubjectLogonId": "999",
                "SubjectUserSid": "S-1-5-18",
                "SubjectDomainName": "NT AUTHORITY"
              }
            },
            {
              "values": {
                "EventID": "4672",
                "PrivilegeList": "SeAssignPrimaryTokenPrivilege\r\n\t\t\tSeTcbPrivilege\r\n\t\t\tSeSecurityPrivilege\r\n\t\t\tSeTakeOwnershipPrivilege\r\n\t\t\tSeLoadDriverPrivilege\r\n\t\t\tSeBackupPrivilege\r\n\t\t\tSeRestorePrivilege\r\n\t\t\tSeDebugPrivilege\r\n\t\t\tSeAuditPrivilege\r\n\t\t\tSeSystemEnvironmentPrivilege\r\n\t\t\tSeImpersonatePrivilege",
                "SubjectUserName": "SYSTEM",
                "SubjectLogonId": "999",
                "SubjectUserSid": "S-1-5-18",
                "SubjectDomainName": "NT AUTHORITY"
              }
            }
          ],
          "rule_level": "low",
          "rule_description": "Detects logon with \"Special groups\" and \"Special Privileges\" can be thought of as Administrator groups or privileges.",
          "rule_source": "Sigma Integrated Rule Set (GitHub)",
          "rule_title": "User with Privileges Logon",
          "rule_id": "8919a871f4a52b7af785fab44b4665ab6a3637e6ebeeac0288df8a5012a48be2",
          "rule_author": "frack113"
        }
      ],
      "last_analysis_stats": {
        "malicious": 62,
        "suspicious": 0,
        "undetected": 2,
        "harmless": 0,
        "timeout": 4,
        "confirmed-timeout": 0,
        "failure": 0,
        "type-unsupported": 6
      },
      "antiy_info": "Trojan/Generic.ASBOL.2A",
      "last_submission_date": 1789584885,
      "popular_threat_classification": {
        "popular_threat_category": [
          {
            "count": 12,
            "value": "virus"
          },
          {
            "count": 1,
            "value": "trojan"
          }
        ],
        "suggested_threat_label": "virus.eicar/test",
        "popular_threat_name": [
          {
            "count": 57,
            "value": "eicar"
          },
          {
            "count": 46,
            "value": "test"
          },
          {
            "count": 34,
            "value": "file"
          }
        ]
      },
      "crowdsourced_ai_results": [
        {
          "source": "palm",
          "verdict": "",
          "analysis": "EICAR is a test string used to detect and test antivirus software. It's like a \"dummy virus\" that triggers an antivirus engine to react, demonstrating its ability to identify and neutralize threats.\nHere's the key:\nIt's NOT a real virus: EICAR is harmless and cannot infect your computer.\nIt's a standardized test: Almost all antivirus programs are designed to recognize EICAR as a potential threat, ensuring they're working properly.\n",
          "category": "code_insight",
          "id": "275a021bbfb6489e54d471899f7db9d1663fc695ec2fe2a2c4538aabf651fd0f-file-palm"
        }
      ]
    }
  }
}

// Case 2 — 2546dc...: malicious detections + Sigma high/medium matches
{
  "data": {
    "id": "2546dcffc5ad854d4ddc64fbf056871cd5a00f2471cb7a5bfd4ac23b6e9eedad",
    "type": "file",
    "links": {
      "self": "https://www.virustotal.com/api/v3/files/2546dcffc5ad854d4ddc64fbf056871cd5a00f2471cb7a5bfd4ac23b6e9eedad"
    },
    "attributes": {
      "tlsh": "T114C08C051A0E9525E00B403827F99DE32A2C60DC08B1AFB8B18C30B48C6B4C90DFFF4C",
      "last_analysis_stats": {
        "malicious": 56,
        "suspicious": 0,
        "undetected": 8,
        "harmless": 0,
        "timeout": 1,
        "confirmed-timeout": 0,
        "failure": 4,
        "type-unsupported": 5
      },
      "sigma_analysis_summary": {
        "Sigma Integrated Rule Set (GitHub)": {
          "high": 1,
          "medium": 1,
          "critical": 0,
          "low": 0
        }
      },
      "filecondis": {
        "dhash": "f0a0904000000000",
        "raw_md5": "1a61638bb1efc994d80fe13e7bcf19b6"
      },
      "times_submitted": 32478,
      "magic": "Zip archive data, at least v1.0 to extract, compression method=store",
      "tags": [
        "via-tor",
        "checks-cpu-name",
        "calls-wmi",
        "attachment",
        "sets-process-name",
        "zip",
        "detect-debug-environment"
      ],
      "sha1": "d27265074c9eac2e2122ed69294dbc4d7cce9141",
      "names": [
        "eicar_com.zip",
        "eicar_com (1).zip",
        "eicar_com(1).zip",
        "eicar-zip.zip",
        "alkn.zip",
        "eicar_com (6).zip",
        "eicar_com (5).zip",
        "document_181458_f49af2ca.zip",
        "eicar_com (2).zip",
        "eicar_com(Copy).sieghail",
        "eicar_com 6.zip",
        "djE8F_uUwGyMK7rY3QfRpTQNkLvVPW9V9mkbHkQ9ORBorBmKeOnluIaSrB3BgKMifjDqPh-tz7Sh7L2H7YCdjG0.zip",
        "eicar_com.pdf",
        "eicar.zip",
        "eicar_com.exe",
        "eicar_com(testviruse).zip",
        "Northfield-Sample-Report.zip",
        "eicar1.zip",
        "zMXjjDMe.zip",
        "eicar_com.bat",
        "file_eicar_com.zip",
        "NexusClient .zip",
        "B1761DA403D497FFBA6063A9C4907CF3",
        "eicar_com(1)(1).zip",
        "Unconfirmed 508065.crdownload",
        "eicar_com[1].zip",
        "eicar_com%20(1).zip",
        "eicar-com(1).zip",
        "eicar_com_16e9a941ca.zip",
        "ui-utils-extra-1.0.0.jar.zip",
        "XInfiniteTickets.pak",
        "eicar_zip.pdf",
        "eicar.com.zip",
        "eicar_com_3.zip",
        "download (1).zip",
        "eicar-zip(2).zip",
        "eicar_com(4).zip",
        "upload-8273993159508884218eicar_com.zip",
        "eicar_com_zip.txt",
        "eicar_com.txt",
        "eicar_com_zip2.txt",
        "eicar-zip(1).zip",
        "muestra_com.zip",
        "62737_20260731101400.zip",
        "eicar_com.jpeg",
        "eicar_com.zip.png",
        "filename.ext",
        "scan_2546dcffc5ad854d4ddc64fbf056871cd5a00f2471cb7a5bfd4ac23b6e9eedad_rf1a2ca7b_f001.zip",
        "fsa-hsa-details.zip",
        "eicar_com - Copy.zip.docx",
        "FILE_A.exe",
        "Unconfirmed 211895.crdownload",
        "eicar_com(7).zip",
        "eicar_com(2).zip",
        "eicar_com TEST1.zip",
        "eicar.csv",
        "test virus.zip",
        "testvirus.zip",
        "testvirus 2.zip",
        "EICAR.zip.htm",
        "eicar_com(8).zip",
        "eicar_com(6).zip",
        "eicar_com (3).zip",
        "eicar_com(3).zip",
        "eicar_com(5).zip",
        "hfsdfasa.zip",
        "eicar_com - Copy.zip",
        "0645fec32744.zip",
        "Unconfirmed 820015.crdownload",
        "UVCyber_eicar_com.zip",
        "hjddfnhjofdsjohht4iuffjdfsjdfsjdfskjlkdfskjldfslkjfdsfkjdslfdkjl.zip",
        "eicar_com (4).zip",
        "calc_infected.apk",
        "eicar_com 3.zip",
        "PathwayDocs_0d708a21-705f-4c58-826e-ff148fb8b0aa__eicar_com.zip",
        "eicar_com 2.zip",
        "I AM A VIRUS.zip",
        "eicar_com.zip.deb",
        "AVIRTEST.ZIP",
        "eicar_com.zip.crdownload",
        "v.zip",
        "eicar.pdf",
        "hehe.zip",
        "eicar_com (33).zip"
      ],
      "first_submission_date": 1148417982,
      "md5": "6ce6f415d8475545be5ba114f208b0ff",
      "type_tag": "zip",
      "last_analysis_results": {
        "Bkav": {
          "method": "blacklist",
          "engine_name": "Bkav",
          "engine_version": "8.2.40(8338)",
          "engine_update": "20260916",
          "category": "undetected",
          "result": null
        },
        "Lionic": {
          "method": "blacklist",
          "engine_name": "Lionic",
          "engine_version": "8.16",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Test.ZIP.Eicar.y!c"
        },
        "Elastic": {
          "method": "blacklist",
          "engine_name": "Elastic",
          "engine_version": "4.0.288",
          "engine_update": "20260910",
          "category": "malicious",
          "result": "eicar"
        },
        "MicroWorld-eScan": {
          "method": "blacklist",
          "engine_name": "MicroWorld-eScan",
          "engine_version": "14.0.409.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "ClamAV": {
          "method": "blacklist",
          "engine_name": "ClamAV",
          "engine_version": "1.5.4.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar-Test-Signature"
        },
        "CMC": {
          "method": "blacklist",
          "engine_name": "CMC",
          "engine_version": "2.4.2022.1",
          "engine_update": "20260915",
          "category": "undetected",
          "result": null
        },
        "CAT-QuickHeal": {
          "method": "blacklist",
          "engine_name": "CAT-QuickHeal",
          "engine_version": "22.00",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR.TestFile"
        },
        "Skyhigh": {
          "method": "blacklist",
          "engine_name": "Skyhigh",
          "engine_version": "v2021.2.0+4045",
          "engine_update": "20260915",
          "category": "malicious",
          "result": "EICAR test file"
        },
        "ALYac": {
          "method": "blacklist",
          "engine_name": "ALYac",
          "engine_version": "2.0.0.10",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Misc.Eicar-Test-File"
        },
        "Malwarebytes": {
          "method": "blacklist",
          "engine_name": "Malwarebytes",
          "engine_version": "3.1.0.272",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-AV-Test"
        },
        "VIPRE": {
          "method": "blacklist",
          "engine_name": "VIPRE",
          "engine_version": "6.0.0.35",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File (not a virus)"
        },
        "Sangfor": {
          "method": "blacklist",
          "engine_name": "Sangfor",
          "engine_version": "2.22.3.0",
          "engine_update": "20260915",
          "category": "malicious",
          "result": "EICAR-Test-File (not a virus)"
        },
        "K7AntiVirus": {
          "method": "blacklist",
          "engine_name": "K7AntiVirus",
          "engine_version": "14.79.60815",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_Test_File"
        },
        "BitDefender": {
          "method": "blacklist",
          "engine_name": "BitDefender",
          "engine_version": "7.2",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File (not a virus)"
        },
        "K7GW": {
          "method": "blacklist",
          "engine_name": "K7GW",
          "engine_version": "14.79.60817",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_Test_File"
        },
        "Trustlook": {
          "method": "blacklist",
          "engine_name": "Trustlook",
          "engine_version": "1.0",
          "engine_update": "20260916",
          "category": "undetected",
          "result": null
        },
        "huorong": {
          "method": "blacklist",
          "engine_name": "huorong",
          "engine_version": "0b31bda:0b31bda:c621e9a:c621e9a",
          "engine_update": "20260915",
          "category": "malicious",
          "result": "TEST/AVEngTestFile!EICAR"
        },
        "VirIT": {
          "method": "blacklist",
          "engine_name": "VirIT",
          "engine_version": "9.5.1295",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "SymantecMobileInsight": {
          "method": "blacklist",
          "engine_name": "SymantecMobileInsight",
          "engine_version": "2.0",
          "engine_update": "20260123",
          "category": "malicious",
          "result": "AppRisk:Generisk"
        },
        "Symantec": {
          "method": "blacklist",
          "engine_name": "Symantec",
          "engine_version": "1.22.0.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR Test String"
        },
        "ESET-NOD32": {
          "method": "blacklist",
          "engine_name": "ESET-NOD32",
          "engine_version": "18.2.18.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar test file"
        },
        "TrendMicro-HouseCall": {
          "method": "blacklist",
          "engine_name": "TrendMicro-HouseCall",
          "engine_version": "24.550.0.1002",
          "engine_update": "20260916",
          "category": "undetected",
          "result": null
        },
        "Cynet": {
          "method": "blacklist",
          "engine_name": "Cynet",
          "engine_version": "4.0.3.4",
          "engine_update": "20260915",
          "category": "malicious",
          "result": "Malicious (score: 99)"
        },
        "Alibaba": {
          "method": "blacklist",
          "engine_name": "Alibaba",
          "engine_version": "0.3.0.5",
          "engine_update": "20190527",
          "category": "malicious",
          "result": "Virus:Win32/EICAR.A"
        },
        "NANO-Antivirus": {
          "method": "blacklist",
          "engine_name": "NANO-Antivirus",
          "engine_version": "1.0.170.26895",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Marker.Dos.EICAR-Test-File.dyb"
        },
        "ViRobot": {
          "method": "blacklist",
          "engine_name": "ViRobot",
          "engine_version": "2014.3.20.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "BIN.S.Eicar.184"
        },
        "Rising": {
          "method": "blacklist",
          "engine_name": "Rising",
          "engine_version": "25.0.0.28",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Virus.EICARTestFile!1.103DB (CLASSIC)"
        },
        "Sophos": {
          "method": "blacklist",
          "engine_name": "Sophos",
          "engine_version": "3.6.2.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-AV-Test"
        },
        "F-Secure": {
          "method": "blacklist",
          "engine_name": "F-Secure",
          "engine_version": "18.10.1547.307",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_Test_File"
        },
        "DrWeb": {
          "method": "blacklist",
          "engine_name": "DrWeb",
          "engine_version": "7.0.76.8280",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR Test File (NOT a Virus!)"
        },
        "Zillya": {
          "method": "blacklist",
          "engine_name": "Zillya",
          "engine_version": "2.0.0.5690",
          "engine_update": "20260915",
          "category": "malicious",
          "result": "EICAR.TestFile"
        },
        "TrendMicro": {
          "method": "blacklist",
          "engine_name": "TrendMicro",
          "engine_version": "24.550.0.1002",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar_test_file"
        },
        "McAfeeD": {
          "method": "blacklist",
          "engine_name": "McAfeeD",
          "engine_version": "1.2.0.16123",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "ti!2546DCFFC5AD"
        },
        "CTX": {
          "method": "blacklist",
          "engine_name": "CTX",
          "engine_version": "2024.8.29.1",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "zip.virus.eicar"
        },
        "Emsisoft": {
          "method": "blacklist",
          "engine_name": "Emsisoft",
          "engine_version": "2024.8.0.61147",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File (not a virus) (B)"
        },
        "Ikarus": {
          "method": "blacklist",
          "engine_name": "Ikarus",
          "engine_version": "6.5.4.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "GData": {
          "method": "blacklist",
          "engine_name": "GData",
          "engine_version": "GD:27.46016AVA:64.31910",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_TEST_FILE"
        },
        "Jiangmin": {
          "method": "blacklist",
          "engine_name": "Jiangmin",
          "engine_version": "16.0.100",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "Webroot": {
          "method": "blacklist",
          "engine_name": "Webroot",
          "engine_version": "1.9.0.8",
          "engine_update": "20250227",
          "category": "malicious",
          "result": "W32.Eicar.Testvirus.Gen"
        },
        "Avira": {
          "method": "blacklist",
          "engine_name": "Avira",
          "engine_version": "8.3.3.24",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar-Test-Signature"
        },
        "Antiy-AVL": {
          "method": "blacklist",
          "engine_name": "Antiy-AVL",
          "engine_version": "3.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "TestFile/Win32.EICAR"
        },
        "Kingsoft": {
          "method": "blacklist",
          "engine_name": "Kingsoft",
          "engine_version": "None",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Win32.Infected.AutoInfector.a"
        },
        "Microsoft": {
          "method": "blacklist",
          "engine_name": "Microsoft",
          "engine_version": "1.26080",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Virus:DOS/EICAR_Test_File"
        },
        "Gridinsoft": {
          "method": "blacklist",
          "engine_name": "Gridinsoft",
          "engine_version": "1.0.254.174",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Trojan.U.EICAR_Test_File.dd"
        },
        "Xcitium": {
          "method": "blacklist",
          "engine_name": "Xcitium",
          "engine_version": "38962",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "ApplicUnwnt@#27s8ewoxds1vr"
        },
        "Arcabit": {
          "method": "blacklist",
          "engine_name": "Arcabit",
          "engine_version": "2025.0.0.23",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File (not a virus)"
        },
        "SUPERAntiSpyware": {
          "method": "blacklist",
          "engine_name": "SUPERAntiSpyware",
          "engine_version": "5.6.0.1032",
          "engine_update": "20260915",
          "category": "malicious",
          "result": "NotAThreat.EICAR[TestFile]"
        },
        "ZoneAlarm": {
          "method": "blacklist",
          "engine_name": "ZoneAlarm",
          "engine_version": "6.27-118568656",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-AV-Test"
        },
        "Avast-Mobile": {
          "method": "blacklist",
          "engine_name": "Avast-Mobile",
          "engine_version": "260916-00",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Eicar"
        },
        "Varist": {
          "method": "blacklist",
          "engine_name": "Varist",
          "engine_version": "6.6.1.3",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_Test_File"
        },
        "AhnLab-V3": {
          "method": "blacklist",
          "engine_name": "AhnLab-V3",
          "engine_version": "3.30.1.10706",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "Virus/EICAR_Test_File"
        },
        "Acronis": {
          "method": "blacklist",
          "engine_name": "Acronis",
          "engine_version": "1.2.0.121",
          "engine_update": "20240328",
          "category": "undetected",
          "result": null
        },
        "VBA32": {
          "method": "blacklist",
          "engine_name": "VBA32",
          "engine_version": "5.7.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-Test-File"
        },
        "TACHYON": {
          "method": "blacklist",
          "engine_name": "TACHYON",
          "engine_version": "2026-09-16.02",
          "engine_update": "20260916",
          "category": "undetected",
          "result": null
        },
        "Zoner": {
          "method": "blacklist",
          "engine_name": "Zoner",
          "engine_version": "2.2.2.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR.Test.File-NoVirus.250"
        },
        "Tencent": {
          "method": "blacklist",
          "engine_name": "Tencent",
          "engine_version": "1.0.0.1",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR.TEST.NOT-A-VIRUS"
        },
        "Yandex": {
          "method": "blacklist",
          "engine_name": "Yandex",
          "engine_version": "5.5.2.24",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_test_file"
        },
        "TrellixENS": {
          "method": "blacklist",
          "engine_name": "TrellixENS",
          "engine_version": "6.0.6.653",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR test file"
        },
        "SentinelOne": {
          "method": "blacklist",
          "engine_name": "SentinelOne",
          "engine_version": "7.7.0.1",
          "engine_update": "20260714",
          "category": "malicious",
          "result": "Static AI - Malicious Archive"
        },
        "MaxSecure": {
          "method": "blacklist",
          "engine_name": "MaxSecure",
          "engine_version": "1.0.0.1",
          "engine_update": "20260916",
          "category": "undetected",
          "result": null
        },
        "Fortinet": {
          "method": "blacklist",
          "engine_name": "Fortinet",
          "engine_version": "7.0.48.0",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR_TEST_FILE"
        },
        "Panda": {
          "method": "blacklist",
          "engine_name": "Panda",
          "engine_version": "4.6.4.2",
          "engine_update": "20260916",
          "category": "malicious",
          "result": "EICAR-AV-TEST-FILE"
        },
        "CrowdStrike": {
          "method": "blacklist",
          "engine_name": "CrowdStrike",
          "engine_version": "1.0",
          "engine_update": "20251219",
          "category": "undetected",
          "result": null
        },
        "alibabacloud": {
          "method": "blacklist",
          "engine_name": "alibabacloud",
          "engine_version": "2.2.0",
          "engine_update": "20250321",
          "category": "malicious",
          "result": "Engtest:Multi/Eicar"
        },
        "Google": {
          "method": "blacklist",
          "engine_name": "Google",
          "engine_version": null,
          "engine_update": "20260916",
          "category": "timeout",
          "result": null
        },
        "Avast": {
          "method": "blacklist",
          "engine_name": "Avast",
          "engine_version": "23.9.8494.0",
          "engine_update": "20260916",
          "category": "failure",
          "result": null
        },
        "AVG": {
          "method": "blacklist",
          "engine_name": "AVG",
          "engine_version": "23.9.8494.0",
          "engine_update": "20260916",
          "category": "failure",
          "result": null
        },
        "Kaspersky": {
          "method": "blacklist",
          "engine_name": "Kaspersky",
          "engine_version": "22.0.1.28",
          "engine_update": "20260916",
          "category": "failure",
          "result": null
        },
        "DeepInstinct": {
          "method": "blacklist",
          "engine_name": "DeepInstinct",
          "engine_version": "5.0.0.8",
          "engine_update": "20260614",
          "category": "failure",
          "result": null
        },
        "APEX": {
          "method": "blacklist",
          "engine_name": "APEX",
          "engine_version": "6.821",
          "engine_update": "20260916",
          "category": "type-unsupported",
          "result": null
        },
        "Paloalto": {
          "method": "blacklist",
          "engine_name": "Paloalto",
          "engine_version": "0.9.0.1003",
          "engine_update": "20260916",
          "category": "type-unsupported",
          "result": null
        },
        "BitDefenderFalx": {
          "method": "blacklist",
          "engine_name": "BitDefenderFalx",
          "engine_version": "2.0.936",
          "engine_update": "20260903",
          "category": "type-unsupported",
          "result": null
        },
        "Cylance": {
          "method": "blacklist",
          "engine_name": "Cylance",
          "engine_version": "3.0.0.0",
          "engine_update": "20260910",
          "category": "type-unsupported",
          "result": null
        },
        "tehtris": {
          "method": "blacklist",
          "engine_name": "tehtris",
          "engine_version": "v0.1.4",
          "engine_update": "20260916",
          "category": "type-unsupported",
          "result": null
        }
      },
      "sha256": "2546dcffc5ad854d4ddc64fbf056871cd5a00f2471cb7a5bfd4ac23b6e9eedad",
      "picklescan": {
        "globals": [
          
        ],
        "contains_invalid_pickle": false,
        "issues_count": 0
      },
      "type_tags": [
        "compressed",
        "zip"
      ],
      "sigma_analysis_results": [
        {
          "match_context": [
            {
              "values": {
                "EventID": "1",
                "Product": "Microsoft\\xae Windows\\xae Operating System",
                "ParentImage": "C:\\Users\\sandbox\\AppData\\Local\\Programs\\Python\\Python38-32\\pythonw.exe",
                "Description": "Audit Policy Program",
                "CurrentDirectory": "C:\\tmpx5trn8j4\\", "Company": "MicrosoftCorporation", "IntegrityLevel": "High", "ParentCommandLine": "C: \\Users\\sandbox\\AppData\\Local\\Programs\\Python\\Python38-32\\pythonw.exeC: /tmpx5trn8j4/analyzer.py", "CommandLine": "auditpol/set/subcategory: \"SAM\" /success:disable /failure:disable",
                "RuleName": "-",
                "Hashes": "MD5=B2575F09B1842015AA2F4A6679B8F611,SHA256=CB2D78BEBD59368EFFB6BB7F2399C0408C0B82990C10C17B62BBD7DC73143A2E,IMPHASH=22D602C3D8E6F25ECB33FDE5F8427BD0",
                "Image": "C:\\Windows\\SysWOW64\\auditpol.exe",
                "OriginalFileName": "AUDITPOL.EXE",
                "FileVersion": "6.1.7601.24335 (win7sp1_ldr_escrow.181228-0954)"
              }
            }
          ],
          "rule_level": "high",
          "rule_id": "33a4a18ae1a3802586c239be79075294541594b5b603c230af39618577e03fae",
          "rule_source": "Sigma Integrated Rule Set (GitHub)",
          "rule_title": "Audit Policy Tampering Via Auditpol",
          "rule_description": "Threat actors can use auditpol binary to change audit policy configuration to impair detection capability.\nThis can be carried out by selectively disabling/removing certain audit policies as well as restoring a custom policy owned by the threat actor.\n",
          "rule_author": "Janantha Marasinghe (https://github.com/blueteam0ps)"
        },
        {
          "match_context": [
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49245",
                "Protocol": "tcp",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49246",
                "SourceIsIpv6": "false",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49247",
                "Protocol": "tcp",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "DestinationIp": "104.208.16.93",
                "Initiated": "true",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49248",
                "SourceIsIpv6": "false",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49250",
                "Protocol": "tcp",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49251",
                "SourceIsIpv6": "false",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49252",
                "Protocol": "tcp",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "DestinationIp": "104.208.16.93",
                "Initiated": "true",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49253",
                "Protocol": "tcp",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49254",
                "SourceIsIpv6": "false",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "DestinationIp": "104.208.16.93",
                "Initiated": "true",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49255",
                "SourceIsIpv6": "false",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49256",
                "Protocol": "tcp",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49257",
                "Protocol": "tcp",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "DestinationIp": "104.208.16.93",
                "Initiated": "true",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49259",
                "SourceIsIpv6": "false",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "DestinationIp": "104.208.16.93",
                "Initiated": "true",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49260",
                "Protocol": "tcp",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49263",
                "SourceIsIpv6": "false",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "DestinationIp": "104.208.16.93",
                "Initiated": "true",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49264",
                "Protocol": "tcp",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49266",
                "Protocol": "tcp",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49267",
                "SourceIsIpv6": "false",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "DestinationIp": "104.208.16.93",
                "Initiated": "true",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49268",
                "SourceIsIpv6": "false",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49269",
                "SourceIsIpv6": "false",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49270",
                "Protocol": "tcp",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "Initiated": "true",
                "DestinationIp": "104.208.16.93",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49271",
                "Protocol": "tcp",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "DestinationIp": "104.208.16.93",
                "Initiated": "true",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49272",
                "SourceIsIpv6": "false",
                "SourceHostname": "-",
                "SourceIp": "172.16.1.3",
                "DestinationIsIpv6": "false"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "DestinationIp": "104.208.16.93",
                "Initiated": "true",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49273",
                "SourceIsIpv6": "false",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "SourceIsIpv6": "false",
                "DestinationIp": "104.208.16.93",
                "Initiated": "true",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49275",
                "Protocol": "tcp",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            },
            {
              "values": {
                "EventID": "3",
                "DestinationPortName": "-",
                "Protocol": "tcp",
                "DestinationIp": "104.208.16.93",
                "Initiated": "true",
                "SourcePortName": "-",
                "Image": "C:\\Windows\\system32\\RunDll32.exe",
                "DestinationHostname": "-",
                "DestinationPort": "443",
                "RuleName": "-",
                "SourcePort": "49276",
                "SourceIsIpv6": "false",
                "DestinationIsIpv6": "false",
                "SourceIp": "172.16.1.3",
                "SourceHostname": "-"
              }
            }
          ],
          "rule_level": "medium",
          "rule_id": "4725cdcf2dfdd90c3aa0d331fae77d6ac8021c254701744a01444af04e9a0e69",
          "rule_source": "Sigma Integrated Rule Set (GitHub)",
          "rule_title": "Rundll32 Internet Connection",
          "rule_description": "Detects a rundll32 that communicates with public IP addresses",
          "rule_author": "Florian Roth (Nextron Systems)"
        }
      ],
      "contenthash": "0412c860cff064ff9d5e9fde10c34fdb",
      "saferpickle": {
        "justification": null,
        "classification": "benign"
      },
      "reputation": 496,
      "unique_sources": 3703,
      "sigma_analysis_stats": {
        "high": 1,
        "medium": 1,
        "critical": 0,
        "low": 0
      },
      "magika": "ZIP",
      "trid": [
        {
          "file_type": "ZIP compressed archive",
          "probability": 100.0
        }
      ],
      "total_votes": {
        "harmless": 179,
        "malicious": 48
      },
      "sandbox_verdicts": {
        "Zenbox Linux": {
          "category": "harmless",
          "confidence": 3,
          "sandbox_name": "Zenbox Linux",
          "malware_classification": [
            "CLEAN"
          ]
        },
        "C2AE": {
          "category": "undetected",
          "sandbox_name": "C2AE",
          "malware_classification": [
            "UNKNOWN_VERDICT"
          ]
        },
        "Zenbox": {
          "category": "malicious",
          "malware_classification": [
            "MALWARE",
            "TROJAN"
          ],
          "sandbox_name": "Zenbox",
          "malware_names": [
            "EICAR"
          ],
          "confidence": 48
        }
      },
      "bundle_info": {
        "highest_datetime": "2000-05-24 19:07:00",
        "lowest_datetime": "2000-05-24 19:07:00",
        "num_children": 1,
        "extensions": {
          "com": 1
        },
        "file_types": {
          "unknown": 1
        },
        "type": "ZIP",
        "uncompressed_size": 68
      },
      "last_analysis_date": 1789565591,
      "popular_threat_classification": {
        "popular_threat_category": [
          {
            "value": "virus",
            "count": 12
          },
          {
            "value": "trojan",
            "count": 1
          }
        ],
        "popular_threat_name": [
          {
            "value": "eicar",
            "count": 49
          },
          {
            "value": "test",
            "count": 38
          },
          {
            "value": "file",
            "count": 29
          }
        ],
        "suggested_threat_label": "virus.eicar/test"
      },
      "first_seen_itw_date": 1580091983,
      "vhash": "197dc9f5077a8db76a97c158fd3cc6b9",
      "last_modification_date": 1789585314,
      "size": 184,
      "type_description": "ZIP",
      "ssdeep": "3:vh9NeYh/CTpA/cAJraNvsgzsVqSwHqXzj9//9eYh/F/aHEpIh/+lIl//:59sYMTpDAJuOgzskozj9sY//zpW+lIt/",
      "meaningful_name": "eicar_com.zip",
      "last_submission_date": 1789585313,
      "type_extension": "zip"
    }
  }
}

// Case 3 - 000000...0001: file not found
{
  "error": {
    "code": "NotFoundError",
    "message": "File \"0000000000000000000000000000000000000000000000000000000000000001\" not found"
  }
}
```
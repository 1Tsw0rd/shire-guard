# test.ollama.sh 실행 결과 예시

> [`scripts/test.ollama.sh`](../../scripts/test.ollama.sh)로 Ollama 모델 2개를 호출하고, `--ko` 옵션으로 응답을 한국어로 번역한 결과

## 테스트 설정

| 상수 | 값 |
|---|---|
| `SYSTEM_PROMPT` | You are a security analysis assistant. Based on the provided evidence, assess the risk as HIGH, MEDIUM, or LOW, and explain your reasoning concisely. |
| `PROMPT` | Review the following file analysis evidence: filename invoice_2026.exe, UPX packing detected, VirusTotal 41/72 detections. |
| `TRANSLATE_MODEL` | `translategemma:12b` |

---

## 1. Qwen2.5 14B (`ollama 1 --ko`)

```bash
./test.ollama.sh ollama 1 --ko
```

```text
▶ Ollama 모델 [qwen2.5:14b-instruct] 호출 중...
{"model":"qwen2.5:14b-instruct",
  "created_at":"2026-10-22T15:18:51.564539527Z",
  "message":{"role":"assistant",
  "content":"**Risk Level: HIGH**\n\n**Reasoning:**\n- **File Type and Name:** The filename `invoice_2026.exe` suggests it is a Windows executable file,
   commonly used for malware distribution as it can appear legitimate at first glance.\n- **UPX Packing:** UPX (Ultimate Packer for Executables) is often used by malware authors to compress and obfuscate malware,
   making it harder to detect. While UPX is also used by legitimate software,
   its presence in suspicious files raises concern.\n- **VirusTotal Detections:** The file received 41/72 detections on VirusTotal,
   which is a significant number of detections from different antivirus engines. This indicates that multiple security vendors have flagged the file as malicious,
   suggesting a high likelihood that the file contains harmful content.\n\nGiven these factors,
   the risk associated with `invoice_2026.exe` is assessed as HIGH."},
  "done":true,
  "done_reason":"stop",
  "total_duration":3513205665,
  "load_duration":625227,
  "prompt_eval_count":77,
  "prompt_eval_duration":218669000,
  "eval_count":188,
  "eval_duration":3231862000}

▶ 한국어 번역 중... (translategemma:12b)
{"model":"translategemma:12b",
  "created_at":"2026-10-22T15:19:00.061390396Z",
  "message":{"role":"assistant",
  "content":"**위험 수준: 높음 (HIGH)**\n\n**이유:**\n- **파일 형식 및 이름:** 파일 이름이 `invoice_2026.exe`로,
   Windows 실행 파일 형식이므로,
   악성코드 배포에 흔히 사용되는 형식이어서 의심스러운 점이 있습니다. 겉보기에는 합법적인 것처럼 보일 수 있습니다.\n- **UPX 압축:** UPX (Ultimate Packer for Executables)는 악성코드 제작자가 악성코드를 압축하고 숨기기 위해 사용하는 경우가 많습니다. UPX는 합법적인 소프트웨어에서도 사용될 수 있지만,
   의심스러운 파일에 존재한다는 것은 우려스러운 점입니다.\n- **VirusTotal 탐지:** VirusTotal에서 이 파일이 72개의 엔진 중 41개에서 악성코드로 탐지되었습니다. 이는 여러 보안 업체에서 이 파일을 악성으로 판단했다는 것을 의미하며,
   파일에 유해한 내용이 포함될 가능성이 매우 높습니다.\n\n이러한 요인들을 종합적으로 고려할 때,
   `invoice_2026.exe` 파일에 대한 위험 수준은 **높음 (HIGH)**으로 평가됩니다.\n"},
  "done":true,
  "done_reason":"stop",
  "total_duration":8329270876,
  "load_duration":4716024247,
  "prompt_eval_count":203,
  "prompt_eval_duration":128727000,
  "eval_count":251,
  "eval_duration":3481422000}
```

---
---
---

## 2. Foundation-Sec 8B Reasoning (`ollama 2 --ko`)

- 추론(Reasoning) 모델이라 응답 앞부분에 판단 과정이 먼저 나오고, `</think>`(응답에서는 `\u003c/think\u003e`) 뒤에 최종 답변이 나옴

```bash
./test.ollama.sh ollama 2 --ko
```

```text
▶ Ollama 모델 [hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF] 호출 중...
{"model":"hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF",
  "created_at":"2026-10-22T15:19:12.197875981Z",
  "message":{"role":"assistant",
  "content":"The file name \"invoice_2026.exe\" is a potential indicator of social engineering,
   as users might be tricked into executing a seemingly legitimate document. The UPX packing is a common obfuscation technique used by malware to evade detection,
   suggesting the file is likely malicious. The high VirusTotal score of 41/72 detections indicates a significant portion of antivirus engines have flagged this fileas malicious. Combining these factors—social engineering lure,
   obfuscation,
   and multiple AV detections—points to a high risk of malware infection.\n\u003c/think\u003e\n\nBased on the provided evidence:\n\n1. **Filename \"invoice_2026.exe\"** suggests social engineering,
   as it mimics a legitimate invoice document. This increases the likelihood of user execution.\n2. **UPX packing** is a common obfuscation technique used by malwareto evade detection and analysis,
   indicating malicious intent.\n3. **41/72 VirusTotal detections** shows 57% of security engines flag the file as malicious,
   confirming widespread threat recognition.\n\n**Risk Assessment: HIGH**  \nThe combination of a deceptive filename,
   obfuscation technique,
   and multiple antivirus detections indicates a high probability of malicious activity."},
  "done":true,
  "done_reason":"stop",
  "total_duration":6092146226,
  "load_duration":2540229618,
  "prompt_eval_count":66,
  "prompt_eval_duration":56308000,
  "eval_count":237,
  "eval_duration":3488645000}

▶ 한국어 번역 중... (translategemma:12b)
{"model":"translategemma:12b",
  "created_at":"2026-10-22T15:19:18.161232304Z",
  "message":{"role":"assistant",
  "content":"다음은 제공된 정보를 바탕으로 작성된 번역입니다.\n\n**\"invoice_2026.exe\" 파일 이름은 사회 공학 공격의 가능성을 나타내는 지표일 수 있습니다. 사용자가 합법적인 문서처럼 보이는 파일을 실행하도록 속일 수 있기 때문입니다. UPX 압축은 악성 프로그램이 탐지를 피하기 위해 사용하는 일반적인 난독화 기술이며,
   이는 해당 파일이 악성일 가능성이 높다는 것을 시사합니다. VirusTotal에서 41/72의 높은 탐지율은 상당수의 백신 엔진이 이 파일을 악성으로 판단하고 있다는 것을 의미합니다. 이러한 요소들(사회 공학적 유인,
   난독화,
   다수의 백신 탐지)을 종합적으로 고려할 때,
   악성 코드 감염의 위험이 매우 높습니다.**\n\n**제공된 증거 기반:**\n\n1. **\"invoice_2026.exe\" 파일 이름:** 합법적인 송장 문서처럼 위장하여 사용자의 실행 가능성을높이는 사회 공학적 기법을 사용했을 가능성이 있습니다.\n2. **UPX 압축:** 악성 프로그램이 탐지와 분석을 회피하기 위해 사용하는 일반적인 난독화 기술로,
   악의적인 의도를 나타냅니다.\n3. **VirusTotal 탐지율 41/72:** 57%의 보안 엔진이 이 파일을 악성으로 판단하여,
   광범위한 위협 인식을 확인합니다.\n\n**위험 평가: 매우 높음 (HIGH)**\n\n기만적인 파일 이름,
   난독화 기술,
   그리고 다수의 백신 탐지 결과를 종합적으로 고려할 때,
   악성 활동의 가능성이 매우 높습니다.\n"},
  "done":true,
  "done_reason":"stop",
  "total_duration":5822098818,
  "load_duration":662803,
  "prompt_eval_count":261,
  "prompt_eval_duration":140983000,
  "eval_count":362,
  "eval_duration":5487719000}
```
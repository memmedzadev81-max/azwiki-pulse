# azwiki-pulse · Bələdçi (Azərbaycanca)

## 1. Layihə nə edir? (sadə dildə)

Vikipediyada hər redaktə bir "event" kimi real vaxtda yayımlanır. Bu axın bütün dünyadakı vikipediyaları əhatə edir, saniyədə onlarla event gəlir. Bizim pipeline bu axına qoşulur və yalnız **Azərbaycan dilli Vikipediyanın** (az.wikipedia.org) redaktələrini götürür. Sonra bu suallara cavab verir:

- Hazırda ən çox hansı mövzular redaktə olunur (trending)?
- Hansı səhifədə **qəfil redaktə partlayışı** var? Məsələn, oyundan sonra futbol klubunun səhifəsi.
- Harada iki redaktor bir-birinin dəyişikliyini geri qaytarır (**edit war**)?
- Hansı səhifənin **adı dəyişib**, məsələn "Gəncə şəhəri" → "Gəncə", və onun tarixçəsi necə qorunur?
- Redaktorlar zamanla necə inkişaf edir: yeni (newcomer) → aktiv (active) → güclü (power)?

**Niyə çətin və fərqlidir:** əksər portfolio layihələri hazır CSV faylı ilə işləyir. Bu layihə isə **canlı axınla** işləyir və real sistemlərin problemlərini həll edir: dublikat event-lər, gecikən event-lər, xarab məlumat, qopan bağlantı, adı dəyişən səhifələr.

## 2. Kompüterdə işə salmaq (Windows)

```
cd "C:\Users\memme\Downloads\azwiki-pulse"
pip install -r requirements.txt
```

**1) İnternetsiz demo.** Ən asan başlanğıcdır. 36 saatlıq süni axın yaradır və nəticəni göstərir:
```
python pulse_cli.py demo
```

**2) Canlı axın.** Əvvəlcə `pulse\config.py` faylında `USER_AGENT` sətrinə öz e-poçtunuzu yazın. Wikimedia bunu tələb edir. Sonra demo-nun yaratdığı `data` qovluğunu silin və işə salın:
```
python pulse_cli.py stream --minutes 15
python pulse_cli.py process
python pulse_cli.py report
```
Azərbaycan Vikipediyası kiçikdir. 15 dəqiqədə bir neçə onlarla redaktə gələ bilər, bu normaldır. Nə qədər uzun işlətsəniz, hesabat bir o qədər maraqlı olar. Spike alert-ləri ən azı 6 saatlıq məlumat yığılandan sonra işləyir.

**3) Testlər:**
```
python -m unittest -v
```

## 3. Qatlar (müsahibə üçün mütləq bilin)

| Qat | Nə edir | Fayl |
|---|---|---|
| **Ingest** | Axına qoşulur, yalnız `azwiki` event-lərini saxlayır, hər 60 saniyədən bir və ya 200 event-dən bir fayl yazır | `pulse/ingest.py` |
| **Bronze** | Xam event-lər, dəyişmədən, `date=.../hour=...` qovluqlarında | `pulse/bronze.py` |
| **Data contract** | Hər event yoxlanır: başlıq var? vaxt düzgündür? wiki doğrudur? | `pulse/contract.py` |
| **Dead-letter** | Yoxlamadan keçməyənlər silinmir, səbəbi ilə ayrıca cədvələ yazılır | `silver_dead_letter` |
| **Silver** | Təmiz, tipli, dublikatsız event-lər | `pulse/silver.py` |
| **Gold** | Star schema: `dim_page`, `dim_user`, `fact_edit`, saatlıq aqreqatlar, alert-lər | `pulse/gold.py` |
| **Quality** | 10 yoxlama. Biri uğursuz olsa, hər şey geri qaytarılır | `pulse/quality.py` |

## 4. Əsas anlayışlar (sadə izahla)

- **SSE (Server-Sent Events):** server bağlantını açıq saxlayır və yeni event-ləri ardıcıl göndərir. Parser-i əl ilə yazmışıq: `pulse/sse.py`.
- **Checkpoint və Last-Event-ID:** proqram dayansa, sonuncu oxunan event-in ID-si yadda qalır. Yenidən başlayanda server həmin yerdən davam edir və heç nə itmir.
- **At-least-once → exactly-once:** checkpoint fayldan **sonra** saxlanılır. Ona görə qəza zamanı bir neçə event təkrar gələ bilər. Silver isə `event_id` ilə dublikatları atır, nəticədə hər event dəqiq bir dəfə yazılır.
- **Atomic write:** fayl əvvəlcə `.tmp` adı ilə yazılır, sonra bir anda adı dəyişdirilir. Yarımçıq fayl heç vaxt oxunmur.
- **Event time və ingestion time:** redaktənin baş verdiyi vaxt ilə bizə çatdığı vaxt fərqli ola bilər. Gecikmə `lateness_s` sütununda saxlanılır.
- **SCD Type 2:** atribut dəyişəndə köhnə sətir silinmir. Köhnə sətir bağlanır (`valid_to`), yenisi açılır.
  - `dim_page`: səhifənin adı dəyişsə də, `page_nk` (daimi açar) eyni qalır.
  - `dim_user`: redaktorun səviyyəsi dəyişəndə yeni versiya yaranır.
- **Point-in-time join:** hər redaktə, baş verdiyi **an** etibarlı olan versiyaya bağlanır.
- **Z-score (spike):** bu saatdakı redaktə sayı ilə əvvəlki 24 saatın ortası və standart sapması müqayisə olunur.
- **Dead-letter queue:** xarab məlumat səssizcə itmir.

## 5. Microsoft Fabric hissəsi
`fabric/` qovluğunda eyni məntiqi **PySpark + Delta Lake** ilə yerinə yetirən iki notebook var:
1. Fabric-də Lakehouse yaradın. `data\bronze` qovluğunu `Files/bronze`-a yükləyin.
2. Workspace → **Import → Notebook** ilə `.ipynb` fayllarını əlavə edin.
3. Lakehouse-u notebook-a qoşun və ardıcıl işə salın (01, sonra 02).

## 6. GitHub-a yükləmək
1. github.com-da yeni **public** repo: `azwiki-pulse`.
2. Faylları yükləyin (`data` qovluğunu atmayın).
3. **Commit changes** basın.

## 7. CV üçün layihə təsviri

**azwiki-pulse – Real-time Wikipedia Streaming Pipeline** | Python, SQL, PySpark, Delta Lake, Microsoft Fabric, Airflow
- Built a streaming pipeline consuming the live Wikimedia event stream (SSE) with checkpointing and resume via Last-Event-ID, achieving exactly-once results through idempotent de-duplication
- Implemented a medallion architecture with a data contract and dead-letter queue, SCD Type 2 dimensions (page renames, editor tiers) and point-in-time correct fact tables that handle late-arriving events
- Detected editing spikes (rolling z-score) and edit wars; ported the transformations to PySpark and Delta MERGE in Microsoft Fabric; orchestrated with Airflow and covered by 14 automated tests in CI

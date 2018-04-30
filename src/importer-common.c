#include "importer-common.h"

bool dryrun = false;
bool verbose = false;
bool debug = false;
bool quiet = false;
bool send_statsd_msgs = true;

int queued_updates = 0;
int queued_inserts = 0;

// utf8 conversion
static char UTF8_DOT[4] = {0xE2, 0x80, 0xA4, '\0' };
static char UTF8_CURRENCY[3] = {0xC2, 0xA4, '\0'};
static char *URI_ESCAPED_DOT = "%2E";
static char *URI_ESCAPED_DOLLAR = "%24";

int replace_dots_and_dollars(char *s)
{
    if (s == NULL) return 0;
    int count = 0;
    char c;
    while ((c = *s) != '\0') {
        if (c == '.' || c == '$') {
            *s = '_';
            count++;
        }
        s++;
    }
    return count;
}

int copy_replace_dots_and_dollars(char* buffer, const char *s)
{
    int len = 0;
    if (s != NULL) {
        char c;
        while ((c = *s) != '\0') {
            if (c == '.') {
                char *p = UTF8_DOT;
                *buffer++ = *p++;
                *buffer++ = *p++;
                *buffer++ = *p;
                len += 3;
            } else if (c == '$') {
                char *p = UTF8_CURRENCY;
                *buffer++ = *p++;
                *buffer++ = *p;
                len += 2;
            } else {
                *buffer++ = c;
                len++;
            }
            s++;
        }
    }
    *buffer = '\0';
    return len;
}

int uri_replace_dots_and_dollars(char* buffer, const char *s)
{
    int len = 0;
    if (s != NULL) {
        char c;
        while ((c = *s) != '\0') {
            if (c == '.') {
                char *p = URI_ESCAPED_DOT;
                *buffer++ = *p++;
                *buffer++ = *p++;
                *buffer++ = *p;
                len += 3;
            } else if (c == '$') {
                char *p = URI_ESCAPED_DOLLAR;
                *buffer++ = *p++;
                *buffer++ = *p++;
                *buffer++ = *p;
                len += 3;
            } else {
                *buffer++ = c;
                len++;
            }
            s++;
        }
    }
    *buffer = '\0';
    return len;
}

static char *win1252_to_utf8[128] = {
    /* 0x80 */	  "\u20AC"   ,   // Euro Sign
    /* 0x81 */	  "\uFFFD"   ,   //
    /* 0x82 */	  "\u201A"   ,   // Single Low-9 Quotation Mark
    /* 0x83 */	  "\u0192"   ,   // Latin Small Letter F With Hook
    /* 0x84 */	  "\u201E"   ,   // Double Low-9 Quotation Mark
    /* 0x85 */	  "\u2026"   ,   // Horizontal Ellipsis
    /* 0x86 */	  "\u2020"   ,   // Dagger
    /* 0x87 */	  "\u2021"   ,   // Double Dagger
    /* 0x88 */	  "\u02C6"   ,   // Modifier Letter Circumflex Accent
    /* 0x89 */	  "\u2030"   ,   // Per Mille Sign
    /* 0x8A */	  "\u0160"   ,   // Latin Capital Letter S With Caron
    /* 0x8B */	  "\u2039"   ,   // Single Left-pointing Angle Quotation Mark
    /* 0x8C */	  "\u0152"   ,   // Latin Capital Ligature Oe
    /* 0x8D */	  "\uFFFD"   ,   //
    /* 0x8E */	  "\u017D"   ,   // Latin Capital Letter Z With Caron
    /* 0x8F */	  "\uFFFD"   ,   //
    /* 0x90 */	  "\uFFFD"   ,   //
    /* 0x91 */	  "\u2018"   ,   // Left Single Quotation Mark
    /* 0x92 */	  "\u2019"   ,   // Right Single Quotation Mark
    /* 0x93 */	  "\u201C"   ,   // Left Double Quotation Mark
    /* 0x94 */	  "\u201D"   ,   // Right Double Quotation Mark
    /* 0x95 */	  "\u2022"   ,   // Bullet
    /* 0x96 */	  "\u2013"   ,   // En Dash
    /* 0x97 */	  "\u2014"   ,   // Em Dash
    /* 0x98 */	  "\u02DC"   ,   // Small Tilde
    /* 0x99 */	  "\u2122"   ,   // Trade Mark Sign
    /* 0x9A */	  "\u0161"   ,   // Latin Small Letter S With Caron
    /* 0x9B */	  "\u203A"   ,   // Single Right-pointing Angle Quotation Mark
    /* 0x9C */	  "\u0153"   ,   // Latin Small Ligature Oe
    /* 0x9D */	  "\uFFFD"   ,   //
    /* 0x9E */	  "\u017E"   ,   // Latin Small Letter Z With Caron
    /* 0x9F */	  "\u0178"   ,   // Latin Capital Letter Y With Diaeresis
    /* 0xA0 */	  "\u00A0"   ,   // No-break Space
    /* 0xA1 */	  "\u00A1"   ,   // Inverted Exclamation Mark
    /* 0xA2 */	  "\u00A2"   ,   // Cent Sign
    /* 0xA3 */	  "\u00A3"   ,   // Pound Sign
    /* 0xA4 */	  "\u00A4"   ,   // Currency Sign
    /* 0xA5 */	  "\u00A5"   ,   // Yen Sign
    /* 0xA6 */	  "\u00A6"   ,   // Broken Bar
    /* 0xA7 */	  "\u00A7"   ,   // Section Sign
    /* 0xA8 */	  "\u00A8"   ,   // Diaeresis
    /* 0xA9 */	  "\u00A9"   ,   // Copyright Sign
    /* 0xAA */	  "\u00AA"   ,   // Feminine Ordinal Indicator
    /* 0xAB */	  "\u00AB"   ,   // Left-pointing Double Angle Quotation Mark
    /* 0xAC */	  "\u00AC"   ,   // Not Sign
    /* 0xAD */	  "\u00AD"   ,   // Soft Hyphen
    /* 0xAE */	  "\u00AE"   ,   // Registered Sign
    /* 0xAF */	  "\u00AF"   ,   // Macron
    /* 0xB0 */	  "\u00B0"   ,   // Degree Sign
    /* 0xB1 */	  "\u00B1"   ,   // Plus-minus Sign
    /* 0xB2 */	  "\u00B2"   ,   // Superscript Two
    /* 0xB3 */	  "\u00B3"   ,   // Superscript Three
    /* 0xB4 */	  "\u00B4"   ,   // Acute Accent
    /* 0xB5 */	  "\u00B5"   ,   // Micro Sign
    /* 0xB6 */	  "\u00B6"   ,   // Pilcrow Sign
    /* 0xB7 */	  "\u00B7"   ,   // Middle Dot
    /* 0xB8 */	  "\u00B8"   ,   // Cedilla
    /* 0xB9 */	  "\u00B9"   ,   // Superscript One
    /* 0xBA */	  "\u00BA"   ,   // Masculine Ordinal Indicator
    /* 0xBB */	  "\u00BB"   ,   // Right-pointing Double Angle Quotation Mark
    /* 0xBC */	  "\u00BC"   ,   // Vulgar Fraction One Quarter
    /* 0xBD */	  "\u00BD"   ,   // Vulgar Fraction One Half
    /* 0xBE */	  "\u00BE"   ,   // Vulgar Fraction Three Quarters
    /* 0xBF */	  "\u00BF"   ,   // Inverted Question Mark
    /* 0xC0 */	  "\u00C0"   ,   // Latin Capital Letter A With Grave
    /* 0xC1 */	  "\u00C1"   ,   // Latin Capital Letter A With Acute
    /* 0xC2 */	  "\u00C2"   ,   // Latin Capital Letter A With Circumflex
    /* 0xC3 */	  "\u00C3"   ,   // Latin Capital Letter A With Tilde
    /* 0xC4 */	  "\u00C4"   ,   // Latin Capital Letter A With Diaeresis
    /* 0xC5 */	  "\u00C5"   ,   // Latin Capital Letter A With Ring Above
    /* 0xC6 */	  "\u00C6"   ,   // Latin Capital Letter Ae
    /* 0xC7 */	  "\u00C7"   ,   // Latin Capital Letter C With Cedilla
    /* 0xC8 */	  "\u00C8"   ,   // Latin Capital Letter E With Grave
    /* 0xC9 */	  "\u00C9"   ,   // Latin Capital Letter E With Acute
    /* 0xCA */	  "\u00CA"   ,   // Latin Capital Letter E With Circumflex
    /* 0xCB */	  "\u00CB"   ,   // Latin Capital Letter E With Diaeresis
    /* 0xCC */	  "\u00CC"   ,   // Latin Capital Letter I With Grave
    /* 0xCD */	  "\u00CD"   ,   // Latin Capital Letter I With Acute
    /* 0xCE */	  "\u00CE"   ,   // Latin Capital Letter I With Circumflex
    /* 0xCF */	  "\u00CF"   ,   // Latin Capital Letter I With Diaeresis
    /* 0xD0 */	  "\u00D0"   ,   // Latin Capital Letter Eth
    /* 0xD1 */	  "\u00D1"   ,   // Latin Capital Letter N With Tilde
    /* 0xD2 */	  "\u00D2"   ,   // Latin Capital Letter O With Grave
    /* 0xD3 */	  "\u00D3"   ,   // Latin Capital Letter O With Acute
    /* 0xD4 */	  "\u00D4"   ,   // Latin Capital Letter O With Circumflex
    /* 0xD5 */	  "\u00D5"   ,   // Latin Capital Letter O With Tilde
    /* 0xD6 */	  "\u00D6"   ,   // Latin Capital Letter O With Diaeresis
    /* 0xD7 */	  "\u00D7"   ,   // Multiplication Sign
    /* 0xD8 */	  "\u00D8"   ,   // Latin Capital Letter O With Stroke
    /* 0xD9 */	  "\u00D9"   ,   // Latin Capital Letter U With Grave
    /* 0xDA */	  "\u00DA"   ,   // Latin Capital Letter U With Acute
    /* 0xDB */	  "\u00DB"   ,   // Latin Capital Letter U With Circumflex
    /* 0xDC */	  "\u00DC"   ,   // Latin Capital Letter U With Diaeresis
    /* 0xDD */	  "\u00DD"   ,   // Latin Capital Letter Y With Acute
    /* 0xDE */	  "\u00DE"   ,   // Latin Capital Letter Thorn
    /* 0xDF */	  "\u00DF"   ,   // Latin Small Letter Sharp S
    /* 0xE0 */	  "\u00E0"   ,   // Latin Small Letter A With Grave
    /* 0xE1 */	  "\u00E1"   ,   // Latin Small Letter A With Acute
    /* 0xE2 */	  "\u00E2"   ,   // Latin Small Letter A With Circumflex
    /* 0xE3 */	  "\u00E3"   ,   // Latin Small Letter A With Tilde
    /* 0xE4 */	  "\u00E4"   ,   // Latin Small Letter A With Diaeresis
    /* 0xE5 */	  "\u00E5"   ,   // Latin Small Letter A With Ring Above
    /* 0xE6 */	  "\u00E6"   ,   // Latin Small Letter Ae
    /* 0xE7 */	  "\u00E7"   ,   // Latin Small Letter C With Cedilla
    /* 0xE8 */	  "\u00E8"   ,   // Latin Small Letter E With Grave
    /* 0xE9 */	  "\u00E9"   ,   // Latin Small Letter E With Acute
    /* 0xEA */	  "\u00EA"   ,   // Latin Small Letter E With Circumflex
    /* 0xEB */	  "\u00EB"   ,   // Latin Small Letter E With Diaeresis
    /* 0xEC */	  "\u00EC"   ,   // Latin Small Letter I With Grave
    /* 0xED */	  "\u00ED"   ,   // Latin Small Letter I With Acute
    /* 0xEE */	  "\u00EE"   ,   // Latin Small Letter I With Circumflex
    /* 0xEF */	  "\u00EF"   ,   // Latin Small Letter I With Diaeresis
    /* 0xF0 */	  "\u00F0"   ,   // Latin Small Letter Eth
    /* 0xF1 */	  "\u00F1"   ,   // Latin Small Letter N With Tilde
    /* 0xF2 */	  "\u00F2"   ,   // Latin Small Letter O With Grave
    /* 0xF3 */	  "\u00F3"   ,   // Latin Small Letter O With Acute
    /* 0xF4 */	  "\u00F4"   ,   // Latin Small Letter O With Circumflex
    /* 0xF5 */	  "\u00F5"   ,   // Latin Small Letter O With Tilde
    /* 0xF6 */	  "\u00F6"   ,   // Latin Small Letter O With Diaeresis
    /* 0xF7 */	  "\u00F7"   ,   // Division Sign
    /* 0xF8 */	  "\u00F8"   ,   // Latin Small Letter O With Stroke
    /* 0xF9 */	  "\u00F9"   ,   // Latin Small Letter U With Grave
    /* 0xFA */	  "\u00FA"   ,   // Latin Small Letter U With Acute
    /* 0xFB */	  "\u00FB"   ,   // Latin Small Letter U With Circumflex
    /* 0xFC */	  "\u00FC"   ,   // Latin Small Letter U With Diaeresis
    /* 0xFD */	  "\u00FD"   ,   // Latin Small Letter Y With Acute
    /* 0xFE */	  "\u00FE"   ,   // Latin Small Letter Thorn
    /* 0xFF */	  "\u00FF"   ,   // Latin Small Letter Y With Diaeresis
};

int convert_to_win1252(const char *str, size_t n, char *utf8)
{
    int j = 0;
    for (int i=0; i < n; i++) {
        uint8_t c = str[i];
        if ((c & 0x80) == 0) { // ascii 7bit
            // handle null characters
            if (c)
                utf8[j++] = c;
            else {
                utf8[j++] = '\\';
                utf8[j++] = 'u';
                utf8[j++] = '0';
                utf8[j++] = '0';
                utf8[j++] = '0';
                utf8[j++] = '0';
           }
        } else { // high bit set
            char *t = win1252_to_utf8[c & 0x7F];
            while ( (c = *t++) ) {
                utf8[j++] = c;
            }
        }
    }
    utf8[j] = '\0';
    return j-1;
}


/* global config */
zconfig_t* config = NULL;
char iso_date_today[ISO_DATE_STR_LEN] = {'0'};
char iso_date_tomorrow[ISO_DATE_STR_LEN] = {'0'};
time_t time_last_tick = 0;

static zfile_t *config_file = NULL;
static const char *config_file_name = NULL;
static time_t config_file_last_modified = 0;
static char *config_file_digest = "";

void config_file_init(const char *file_name)
{
    config_file_name = file_name;
    config_file = zfile_new(NULL, file_name);
    config_file_last_modified = zfile_modified(config_file);
    config_file_digest = strdup(zfile_digest(config_file));
}

bool config_file_has_changed()
{
    bool changed = false;
    zfile_restat(config_file);
    if (config_file_last_modified != zfile_modified(config_file)) {
        const char *new_digest = zfile_digest(config_file);
        // printf("[D] old digest: %s\n[D] new digest: %s\n", config_file_digest, new_digest);
        changed = strcmp(config_file_digest, new_digest) != 0;
    }
    return changed;
}

bool config_update_date_info()
{
    char old_date[ISO_DATE_STR_LEN];
    strcpy(old_date, iso_date_today);

    time_last_tick = time(NULL);
    struct tm lt;
    assert( localtime_r(&time_last_tick, &lt) );
    // calling mktime fills in potentially missing TZ and DST info
    assert( mktime(&lt) != -1 );
    sprintf(iso_date_today,  "%04d-%02d-%02d", 1900 + lt.tm_year, 1 + lt.tm_mon, lt.tm_mday);

    // calculate toomorrows date
    lt.tm_mday += 1;
    assert( mktime(&lt) != -1 );

    sprintf(iso_date_tomorrow,  "%04d-%02d-%02d", 1900 + lt.tm_year, 1 + lt.tm_mon, lt.tm_mday);

    bool changed = strcmp(old_date, iso_date_today);

    // printf("[D] date info: old        ISO date is %s\n", old_date);
    // printf("[D] date info: today's    ISO date is %s\n", iso_date_today);
    // printf("[D] date info: tomorrow's ISO date is %s\n", iso_date_tomorrow);
    // printf("[D] date info: changed             is %d\n", changed);

    return changed;
}

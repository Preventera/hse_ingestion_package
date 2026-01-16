-- OSHA Data Integration - SafetyGraph
-- 2026-01-16 08:36

CREATE TABLE IF NOT EXISTS osha_sectors (
    id SERIAL PRIMARY KEY,
    naics_code VARCHAR(10),
    sector_name VARCHAR(100),
    establishments INTEGER,
    employees BIGINT,
    injuries INTEGER,
    deaths INTEGER,
    dafw_cases INTEGER,
    source VARCHAR(50) DEFAULT 'OSHA_ITA_2020-2024',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELETE FROM osha_sectors WHERE source = 'OSHA_ITA_2020-2024';
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('11', 'Agriculture', 11106, 1829106, 15172, 66, 14681);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('14', 'Other', 1, 21, 2, 0, 0);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('16', 'Other', 1, 5, 2, 0, 0);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('17', 'Other', 5, 176, 6, 0, 4);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('21', 'Mining', 1756, 262614, 2762, 11, 859);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('22', 'Utilities', 11539, 1693203, 17066, 27, 8582);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('23', 'Construction', 69094, 211442596, 98228, 327, 48845);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('24', 'Other', 1, 9, 2, 0, 0);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('26', 'Other', 1, 29, 1, 0, 0);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('27', 'Other', 1, 279, 1, 0, 2);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('31', 'Manufacturing', 15655, 4482134, 19104, 60, 32736);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('32', 'Manufacturing', 41284, 79575007, 54218, 121, 41684);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('33', 'Manufacturing', 60373, 297749302, 78478, 139, 77535);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('34', 'Other', 2, 1139, 3, 0, 2);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('35', 'Other', 11, 735, 18, 0, 4);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('37', 'Other', 10, 454, 18, 0, 1);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('42', 'Wholesale', 42910, 3869194, 65611, 72, 37864);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('44', 'Retail', 45627, 3539742, 59880, 43, 51214);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('45', 'Retail', 12803, 1625786, 16346, 8, 42961);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('46', 'Other', 3, 624, 3, 0, 9);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('48', 'Transportation', 24224, 3061962, 32660, 149, 51661);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('49', 'Warehousing', 13429, 1899721, 17640, 38, 38447);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('50', 'Other', 2, 33, 4, 0, 0);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('51', 'Information', 2463, 404089, 3959, 6, 1479);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('52', 'Finance', 1315, 619886, 2310, 1, 352);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('53', 'Real Estate', 10537, 1913346, 17637, 17, 6256);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('54', 'Professional', 4944, 982740, 8026, 9, 2430);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('55', 'Management', 3054, 1332886964, 5416, 2, 1265);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('56', 'Admin Services', 20072, 1038325691, 28785, 91, 20599);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('59', 'Other', 5, 84, 8, 0, 0);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('60', 'Other', 1, 65, 1, 0, 0);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('61', 'Education', 6368, 2226878, 8748, 10, 9026);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('62', 'Health Care', 56724, 196061882, 73063, 266, 276487);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('63', 'Other', 3, 446, 3, 0, 1);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('71', 'Arts', 4510, 24049540, 6334, 18, 8045);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('72', 'Hospitality', 16829, 2212307, 24692, 27, 18170);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('74', 'Other', 1, 306, 1, 0, 4);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('81', 'Other Services', 9196, 695735, 13674, 17, 5070);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('83', 'Other', 3, 696, 3, 0, 0);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('87', 'Other', 20, 1452, 30, 0, 6);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('90', 'Other', 2, 159, 2, 0, 8);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('92', 'Other', 13836, 371661828, 19872, 138, 52203);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('95', 'Other', 1, 1401, 1, 0, 19);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('96', 'Other', 1, 12, 1, 0, 2);
INSERT INTO osha_sectors (naics_code, sector_name, establishments, employees, injuries, deaths, dafw_cases) VALUES ('99', 'Other', 277, 18848, 425, 0, 211);

SELECT 'OSHA: ' || COUNT(*) || ' secteurs charges' as status FROM osha_sectors;

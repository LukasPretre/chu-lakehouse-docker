"""
Référentiel géographique partagé (J05) : département -> région, et dérivation du département
à partir d'un code commune INSEE ou d'un code postal.

À importer dans les jobs qui ont besoin de la région (J14, J22, J17, J18) :
    import sys; sys.path.insert(0, "/jobs")
    from lib_geo import departement_depuis_commune, departement_depuis_code_postal

Les fonctions renvoient des expressions Spark (Column) : aucune UDF Python, donc rien
à distribuer aux exécuteurs.

Règles de dérivation
- Code commune INSEE (5 caractères) : 3 premiers caractères si le code commence par 97 ou 98
  (outre-mer), 99 si le code commence par 99 (pays étranger), sinon 2 premiers (2A et 2B
  compris pour la Corse).
- Code postal (5 chiffres) : 3 premiers chiffres pour 97x et 98x ; Corse : 200 et 201 -> 2A,
  202 à 206 -> 2B ; sinon 2 premiers chiffres. C'est une approximation : quelques codes
  postaux desservent des communes d'un département voisin. À utiliser seulement en repli,
  quand le code commune manque.
- Un code absent, mal formé, ou dont le département n'existe pas dans le référentiel (20 corse
  numérique, 00, 980 Monaco...) donne NULL (région inconnue -1 en Gold).
"""
from pyspark.sql import functions as F

# Généré depuis @etalab/decoupage-administratif 6.0.0 (construit sur le COG de l'INSEE).
# (code_departement, libelle_departement, code_region_insee, libelle_region, type)
# type : DEPARTEMENT (rattaché à l'une des 18 régions), COM (collectivité d'outre-mer,
# hors des 18 régions : région inconnue -1), ETRANGER (codes INSEE 99xxx des pays étrangers).
DEPARTEMENTS = [
    ('01', 'Ain', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('02', 'Aisne', '32', 'Hauts-de-France', 'DEPARTEMENT'),
    ('03', 'Allier', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('04', 'Alpes-de-Haute-Provence', '93', "Provence-Alpes-Côte d'Azur", 'DEPARTEMENT'),
    ('05', 'Hautes-Alpes', '93', "Provence-Alpes-Côte d'Azur", 'DEPARTEMENT'),
    ('06', 'Alpes-Maritimes', '93', "Provence-Alpes-Côte d'Azur", 'DEPARTEMENT'),
    ('07', 'Ardèche', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('08', 'Ardennes', '44', 'Grand Est', 'DEPARTEMENT'),
    ('09', 'Ariège', '76', 'Occitanie', 'DEPARTEMENT'),
    ('10', 'Aube', '44', 'Grand Est', 'DEPARTEMENT'),
    ('11', 'Aude', '76', 'Occitanie', 'DEPARTEMENT'),
    ('12', 'Aveyron', '76', 'Occitanie', 'DEPARTEMENT'),
    ('13', 'Bouches-du-Rhône', '93', "Provence-Alpes-Côte d'Azur", 'DEPARTEMENT'),
    ('14', 'Calvados', '28', 'Normandie', 'DEPARTEMENT'),
    ('15', 'Cantal', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('16', 'Charente', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('17', 'Charente-Maritime', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('18', 'Cher', '24', 'Centre-Val de Loire', 'DEPARTEMENT'),
    ('19', 'Corrèze', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('21', "Côte-d'Or", '27', 'Bourgogne-Franche-Comté', 'DEPARTEMENT'),
    ('22', "Côtes-d'Armor", '53', 'Bretagne', 'DEPARTEMENT'),
    ('23', 'Creuse', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('24', 'Dordogne', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('25', 'Doubs', '27', 'Bourgogne-Franche-Comté', 'DEPARTEMENT'),
    ('26', 'Drôme', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('27', 'Eure', '28', 'Normandie', 'DEPARTEMENT'),
    ('28', 'Eure-et-Loir', '24', 'Centre-Val de Loire', 'DEPARTEMENT'),
    ('29', 'Finistère', '53', 'Bretagne', 'DEPARTEMENT'),
    ('2A', 'Corse-du-Sud', '94', 'Corse', 'DEPARTEMENT'),
    ('2B', 'Haute-Corse', '94', 'Corse', 'DEPARTEMENT'),
    ('30', 'Gard', '76', 'Occitanie', 'DEPARTEMENT'),
    ('31', 'Haute-Garonne', '76', 'Occitanie', 'DEPARTEMENT'),
    ('32', 'Gers', '76', 'Occitanie', 'DEPARTEMENT'),
    ('33', 'Gironde', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('34', 'Hérault', '76', 'Occitanie', 'DEPARTEMENT'),
    ('35', 'Ille-et-Vilaine', '53', 'Bretagne', 'DEPARTEMENT'),
    ('36', 'Indre', '24', 'Centre-Val de Loire', 'DEPARTEMENT'),
    ('37', 'Indre-et-Loire', '24', 'Centre-Val de Loire', 'DEPARTEMENT'),
    ('38', 'Isère', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('39', 'Jura', '27', 'Bourgogne-Franche-Comté', 'DEPARTEMENT'),
    ('40', 'Landes', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('41', 'Loir-et-Cher', '24', 'Centre-Val de Loire', 'DEPARTEMENT'),
    ('42', 'Loire', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('43', 'Haute-Loire', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('44', 'Loire-Atlantique', '52', 'Pays de la Loire', 'DEPARTEMENT'),
    ('45', 'Loiret', '24', 'Centre-Val de Loire', 'DEPARTEMENT'),
    ('46', 'Lot', '76', 'Occitanie', 'DEPARTEMENT'),
    ('47', 'Lot-et-Garonne', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('48', 'Lozère', '76', 'Occitanie', 'DEPARTEMENT'),
    ('49', 'Maine-et-Loire', '52', 'Pays de la Loire', 'DEPARTEMENT'),
    ('50', 'Manche', '28', 'Normandie', 'DEPARTEMENT'),
    ('51', 'Marne', '44', 'Grand Est', 'DEPARTEMENT'),
    ('52', 'Haute-Marne', '44', 'Grand Est', 'DEPARTEMENT'),
    ('53', 'Mayenne', '52', 'Pays de la Loire', 'DEPARTEMENT'),
    ('54', 'Meurthe-et-Moselle', '44', 'Grand Est', 'DEPARTEMENT'),
    ('55', 'Meuse', '44', 'Grand Est', 'DEPARTEMENT'),
    ('56', 'Morbihan', '53', 'Bretagne', 'DEPARTEMENT'),
    ('57', 'Moselle', '44', 'Grand Est', 'DEPARTEMENT'),
    ('58', 'Nièvre', '27', 'Bourgogne-Franche-Comté', 'DEPARTEMENT'),
    ('59', 'Nord', '32', 'Hauts-de-France', 'DEPARTEMENT'),
    ('60', 'Oise', '32', 'Hauts-de-France', 'DEPARTEMENT'),
    ('61', 'Orne', '28', 'Normandie', 'DEPARTEMENT'),
    ('62', 'Pas-de-Calais', '32', 'Hauts-de-France', 'DEPARTEMENT'),
    ('63', 'Puy-de-Dôme', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('64', 'Pyrénées-Atlantiques', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('65', 'Hautes-Pyrénées', '76', 'Occitanie', 'DEPARTEMENT'),
    ('66', 'Pyrénées-Orientales', '76', 'Occitanie', 'DEPARTEMENT'),
    ('67', 'Bas-Rhin', '44', 'Grand Est', 'DEPARTEMENT'),
    ('68', 'Haut-Rhin', '44', 'Grand Est', 'DEPARTEMENT'),
    ('69', 'Rhône', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('70', 'Haute-Saône', '27', 'Bourgogne-Franche-Comté', 'DEPARTEMENT'),
    ('71', 'Saône-et-Loire', '27', 'Bourgogne-Franche-Comté', 'DEPARTEMENT'),
    ('72', 'Sarthe', '52', 'Pays de la Loire', 'DEPARTEMENT'),
    ('73', 'Savoie', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('74', 'Haute-Savoie', '84', 'Auvergne-Rhône-Alpes', 'DEPARTEMENT'),
    ('75', 'Paris', '11', 'Île-de-France', 'DEPARTEMENT'),
    ('76', 'Seine-Maritime', '28', 'Normandie', 'DEPARTEMENT'),
    ('77', 'Seine-et-Marne', '11', 'Île-de-France', 'DEPARTEMENT'),
    ('78', 'Yvelines', '11', 'Île-de-France', 'DEPARTEMENT'),
    ('79', 'Deux-Sèvres', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('80', 'Somme', '32', 'Hauts-de-France', 'DEPARTEMENT'),
    ('81', 'Tarn', '76', 'Occitanie', 'DEPARTEMENT'),
    ('82', 'Tarn-et-Garonne', '76', 'Occitanie', 'DEPARTEMENT'),
    ('83', 'Var', '93', "Provence-Alpes-Côte d'Azur", 'DEPARTEMENT'),
    ('84', 'Vaucluse', '93', "Provence-Alpes-Côte d'Azur", 'DEPARTEMENT'),
    ('85', 'Vendée', '52', 'Pays de la Loire', 'DEPARTEMENT'),
    ('86', 'Vienne', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('87', 'Haute-Vienne', '75', 'Nouvelle-Aquitaine', 'DEPARTEMENT'),
    ('88', 'Vosges', '44', 'Grand Est', 'DEPARTEMENT'),
    ('89', 'Yonne', '27', 'Bourgogne-Franche-Comté', 'DEPARTEMENT'),
    ('90', 'Territoire de Belfort', '27', 'Bourgogne-Franche-Comté', 'DEPARTEMENT'),
    ('91', 'Essonne', '11', 'Île-de-France', 'DEPARTEMENT'),
    ('92', 'Hauts-de-Seine', '11', 'Île-de-France', 'DEPARTEMENT'),
    ('93', 'Seine-Saint-Denis', '11', 'Île-de-France', 'DEPARTEMENT'),
    ('94', 'Val-de-Marne', '11', 'Île-de-France', 'DEPARTEMENT'),
    ('95', "Val-d'Oise", '11', 'Île-de-France', 'DEPARTEMENT'),
    ('971', 'Guadeloupe', '01', 'Guadeloupe', 'DEPARTEMENT'),
    ('972', 'Martinique', '02', 'Martinique', 'DEPARTEMENT'),
    ('973', 'Guyane', '03', 'Guyane', 'DEPARTEMENT'),
    ('974', 'La Réunion', '04', 'La Réunion', 'DEPARTEMENT'),
    ('975', 'Saint-Pierre-et-Miquelon', None, None, 'COM'),
    ('976', 'Mayotte', '06', 'Mayotte', 'DEPARTEMENT'),
    ('977', 'Saint-Barthélemy', None, None, 'COM'),
    ('978', 'Saint-Martin', None, None, 'COM'),
    ('984', 'Terres australes et antarctiques françaises', None, None, 'COM'),
    ('986', 'Wallis et Futuna', None, None, 'COM'),
    ('987', 'Polynésie française', None, None, 'COM'),
    ('988', 'Nouvelle-Calédonie', None, None, 'COM'),
    ('989', 'Île de Clipperton', None, None, 'COM'),
    ('99', 'Étranger', '99', 'Étranger', 'ETRANGER'),
]


def _nettoyer(col, longueur):
    """Texte sans espaces, zéros initiaux restitués si la valeur est numérique (Excel les perd)."""
    c = F.trim(F.col(col).cast("string")) if isinstance(col, str) else F.trim(col.cast("string"))
    c = F.when(c.rlike("^[0-9]+$"), F.lpad(c, longueur, "0")).otherwise(F.upper(c))
    return F.when((c == "") | c.isNull(), F.lit(None)).otherwise(c)


CODES_CONNUS = [d[0] for d in DEPARTEMENTS]


def _connu(expr):
    """Garde le département seulement s'il existe dans le référentiel, sinon NULL.
    Écarte par exemple 20 (ancien code commune corse numérique, ambigu entre 2A et 2B),
    00 (commune 00000) ou 980 (Monaco)."""
    return F.when(expr.isin(*CODES_CONNUS), expr)


def departement_depuis_commune(col):
    c = _nettoyer(col, 5)
    return _connu(F.when(~c.rlike("^([0-9]{5}|2[AB][0-9]{3})$"), F.lit(None))
                   .when(c.startswith("99"), F.lit("99"))
                   .when(c.startswith("97") | c.startswith("98"), c.substr(1, 3))
                   .otherwise(c.substr(1, 2)))


def departement_depuis_code_postal(col):
    c = _nettoyer(col, 5)
    trois = c.substr(1, 3)
    return _connu(F.when(~c.rlike("^[0-9]{5}$"), F.lit(None))
                   .when(trois.isin("200", "201"), F.lit("2A"))
                   .when(trois.isin("202", "203", "204", "205", "206"), F.lit("2B"))
                   .when(c.startswith("97") | c.startswith("98"), trois)
                   .otherwise(c.substr(1, 2)))


def table_departements(spark):
    """DataFrame du référentiel (110 lignes)."""
    schema = ("code_departement string, libelle_departement string, "
              "code_region_insee string, libelle_region string, type string")
    return spark.createDataFrame(DEPARTEMENTS, schema)

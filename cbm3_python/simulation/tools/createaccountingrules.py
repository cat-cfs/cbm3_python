# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import csv
from cbm3_python.util import loghelper


class CreateAccountingRules(object):

    def __init__(self, projectAccessDb, dist_classes_path, dist_rules_path):
        self.projectAccessDb = projectAccessDb
        self.dist_classes_path = dist_classes_path
        self.dist_rules_path = dist_rules_path

    def createAccountingRulesTables(self):
        table_defs = [
            (
                "tblAccountingRuleTrackingType",
                """
                CREATE TABLE {} (
                    AccountingRuleTrackingTypeID AUTOINCREMENT PRIMARY KEY,
                    name VARCHAR UNIQUE NOT NULL)
                """
            ), (
                "tblAccountingRuleSet",
                """
                CREATE TABLE {} (
                    AccountingRuleSetID AUTOINCREMENT PRIMARY KEY,
                    DistTypeID SHORT NOT NULL REFERENCES tblDisturbanceType (DistTypeID),
                    SPUID LONG REFERENCES tblSPU (SPUID),
                    AccountingRuleTrackingTypeID INTEGER NOT NULL REFERENCES tblAccountingRuleTrackingType (AccountingRuleTrackingTypeID),
                    CONSTRAINT rs_dist_spu_unique UNIQUE (DistTypeID, SPUID))
                """
            ), (
                "tblAccountingRuleType",
                """
                CREATE TABLE {} (
                    AccountingRuleTypeID AUTOINCREMENT PRIMARY KEY,
                    name VARCHAR UNIQUE NOT NULL)
                """
            ), (
                "tblAccountingRule",
                """
                CREATE TABLE {} (
                    AccountingRuleID AUTOINCREMENT PRIMARY KEY,
                    AccountingRuleSetID INTEGER NOT NULL REFERENCES tblAccountingRuleSet (AccountingRuleSetID),
                    AccountingRuleTypeID INTEGER NOT NULL REFERENCES tblAccountingRuleType (AccountingRuleTypeID),
                    rule_value FLOAT NOT NULL)
                """
            ), (
                "tblDisturbanceClass",
                """
                CREATE TABLE {} (
                    id AUTOINCREMENT PRIMARY KEY,
                    name TEXT)
                """
            ), (
                "tblDisturbanceTypeClassification",
                """
                CREATE TABLE {} (
                    DefaultDistTypeID SHORT,
                    disturbance_class_id INTEGER NOT NULL REFERENCES tblDisturbanceClass(id),
                    PRIMARY KEY (DefaultDistTypeID, disturbance_class_id))
                """
            )
        ]

        table_defs.reverse()
        for table, _ in table_defs:
            if self.projectAccessDb.tableExists(table):
                self.projectAccessDb.ExecuteQuery("DROP TABLE {}".format(table))

        table_defs.reverse()
        for table, ddl in table_defs:
            self.projectAccessDb.ExecuteQuery(ddl.format(table))

    def get_makelist_value(self, spu):
        return self.projectAccessDb.Query(
            """
            SELECT
                c.spuid,
                c.total_ag_biomass_c / a.total_area AS val
            FROM (
                SELECT
                    s.spuid,
                    SUM((  st.merchantablebiomasscarbon
                         + st.foliagebiomasscarbon
                         + st.submerchantablebiomasscarbon
                         + otherbiomasscarbon) * s.svoarea) AS total_ag_biomass_c
                FROM tblsvlbyspeciestype st
                INNER JOIN tblsvlattributes s
                    ON st.svoid = s.svoid
                WHERE s.spuid = ?
                GROUP BY s.spuid
            ) AS c
            INNER JOIN (
                SELECT
                    s.spuid,
                    SUM(s.svoarea) AS total_area
                FROM tblsvlattributes s
                GROUP BY s.spuid
            ) AS a
                ON c.spuid = a.spuid
            """,
            [spu]).fetchone().val

    def get_or_add_rule_type(self, rule_type):
        rule_query = "SELECT accountingruletypeid FROM tblaccountingruletype WHERE name LIKE ?"
        existing = self.projectAccessDb.Query(rule_query, [rule_type]).fetchone()
        if existing:
            return existing.accountingruletypeid

        self.projectAccessDb.ExecuteQuery("INSERT INTO tblaccountingruletype (name) VALUES (?)", [rule_type])
        result = self.projectAccessDb.Query(rule_query, [rule_type])
        return result.fetchone().accountingruletypeid

    def get_or_add_rule_sets(self, category, rule_tracking_type, ru=None, spu=None):
        get_rule_sets_query = \
            """
            SELECT rs.accountingrulesetid, rs.spuid
            FROM tblspu spu
            INNER JOIN (
                tblaccountingruleset rs
                INNER JOIN (
                    tbldisturbancetype dt
                    INNER JOIN (
                        tbldisturbanceclass dc
                        INNER JOIN tbldisturbancetypeclassification dtc
                            ON dc.id = dtc.disturbance_class_id
                    ) ON dt.defaultdisttypeid = dtc.defaultdisttypeid
                ) ON rs.disttypeid = dt.disttypeid
            ) ON spu.spuid = rs.spuid
            WHERE dc.name LIKE ?
            """

        add_rule_sets_query = \
            """
            INSERT INTO tblaccountingruleset (disttypeid, spuid, accountingruletrackingtypeid)
            SELECT dt.disttypeid, spu.spuid, tt.accountingruletrackingtypeid
            FROM
                tblaccountingruletrackingtype tt,
                tblspu spu,
                tbldisturbancetype dt
                INNER JOIN (
                    tbldisturbanceclass dc
                    INNER JOIN tbldisturbancetypeclassification dtc
                        ON dc.id = dtc.disturbance_class_id
                ) ON dt.defaultdisttypeid = dtc.defaultdisttypeid
            WHERE dc.name LIKE ?
                AND tt.name LIKE ?
            """

        get_query_params = [category]
        add_query_params = [category, rule_tracking_type]
        if ru is not None:
            get_rule_sets_query = " ".join([get_rule_sets_query, "AND spu.defaultspuid = ?"])
            add_rule_sets_query = " ".join([add_rule_sets_query, "AND spu.defaultspuid = ?"])
            get_query_params.append(ru)
            add_query_params.append(ru)

        if spu is not None:
            get_rule_sets_query = " ".join([get_rule_sets_query, "AND rs.spuid = ?"])
            add_rule_sets_query = " ".join([add_rule_sets_query, "AND spu.spuid = ?"])
            get_query_params.append(spu)
            add_query_params.append(spu)

        existing = self.projectAccessDb.Query(get_rule_sets_query, get_query_params).fetchall()
        if existing:
            return existing

        self.projectAccessDb.ExecuteQuery(add_rule_sets_query, add_query_params)
        result = self.projectAccessDb.Query(get_rule_sets_query, get_query_params)
        return result.fetchall()

    def create_accounting_rules(self):
        self.createAccountingRulesTables()
        loghelper.get_logger().info("  Creating kf5 accounting rules")

        self.projectAccessDb.ExecuteMany(
            "INSERT INTO tblaccountingruletype (name) VALUES (?)",
            [["years_since_disturbance"], ["ag_biomass"], ["total_biomass"], ["total_eco"],
             ["years_since_last_pass_disturbance"], ["ag_biomass_last_pass_disturbance"],
             ["total_biomass_last_pass_disturbance"], ["total_eco_last_pass_disturbance"]])

        self.projectAccessDb.ExecuteMany(
            "INSERT INTO tblAccountingRuleTrackingType (name) VALUES (?)",
            [["ignore"], ["replace"], ["inherit"], ["passive"]])

        with open(self.dist_classes_path) as dist_class_file:
            reader = csv.DictReader(dist_class_file)

            uniqueDisturbanceClasses = set([row["Category"] for row in reader])
            self.projectAccessDb.ExecuteMany(
            "INSERT INTO tbldisturbanceclass (name) VALUES (?)",
            [[dc] for dc in uniqueDisturbanceClasses])

        with open(self.dist_classes_path) as dist_class_file:
            reader = csv.DictReader(dist_class_file)

            self.projectAccessDb.ExecuteMany(
                """
                INSERT INTO tbldisturbancetypeclassification (defaultdisttypeid, disturbance_class_id)
                SELECT ? AS defaultdisttypeid, id AS disturbance_class_id
                FROM tbldisturbanceclass
                WHERE name LIKE ?
                """,
                [[row["DefaultDistTypeID"], row["Category"]] for row in reader])

        with open(self.dist_rules_path) as csvfile:
            add_rule_sql = \
                """
                INSERT INTO tblaccountingrule (accountingrulesetid, accountingruletypeid, rule_value)
                VALUES (?, ?, ?)
                """

            reader = csv.DictReader(csvfile)
            for row in reader:
                dist_class         = row["disturbance_class"]
                rule_tracking_type = row["rule_tracking_type"]
                rule_type          = row.get("rule_type")
                rule_value         = row.get("rule_value")

                ru = row.get("defaultSPUID")
                spu = row.get("SPUID")
                if spu:
                    rule_sets = self.get_or_add_rule_sets(dist_class, rule_tracking_type, spu=spu)
                elif ru:
                    rule_sets = self.get_or_add_rule_sets(dist_class, rule_tracking_type, ru=ru)
                else:
                    rule_sets = self.get_or_add_rule_sets(dist_class, rule_tracking_type)

                if not rule_type:
                    # Rule set with no rules (i.e. to set rule_tracking_type only)
                    continue

                rule_type_id = self.get_or_add_rule_type(rule_type)
                for rule_set in rule_sets:
                    value = self.get_makelist_value(rule_set.spuid) if rule_value == "makelist" else rule_value
                    self.projectAccessDb.ExecuteQuery(
                        add_rule_sql,
                        [rule_set.accountingrulesetid, rule_type_id, value])

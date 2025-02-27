"""
/calculate-metrics/{84}
/calculate-metric/yield_kgha/84/
/clean/84
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2
import pandas as pd


# Function to check the quality of life
def check_quality_of_life(response):
    # Normalize the response to lowercase and check if it is 'very good' or 'good'
    return str(response) == "1" or str(response) == "2"


# Function to check food security
def check_food_security(response):
    # Check if the response for food security is '1', indicating no days of food insecurity
    return str(response) == "1"


def check_safe_water_access(response):
    # Check if the response for safe water access is '1', '2', or '3'
    return str(response) == "1" or str(response) == "2" or str(response) == "3"


def check_farm_injury_free(response):
    # Check if the response for farm injuries is '0'
    return str(response) == "0"


# Function to check if there is affordable medical access
def check_affordable_medical_access(response):
    # Check if the response for affordable medical access is '1' or '2'
    return str(response) == "1" or str(response) == "2"


# Function to check if there is medical services access
def check_medical_services_access(response):
    # Check if the response for medical services access is '1' or '2'
    return str(response) == "1" or str(response) == "2"


def check_women_decision_maker(response):
    # Check if the response for being a woman decision-maker is '2'
    return str(response) == "2"


# Function to calculate the number of women attending training on the farm
def calculate_training_women_attendees_count(
    initial_attendee_1,
    initial_attendee_2,
    initial_attendee_3,
    gender_initial_1,
    gender_initial_2,
    gender_initial_3,
    additional_attendee_1,
    additional_attendee_2,
    additional_attendee_3,
    gender_additional_1,
    gender_additional_2,
    gender_additional_3,
):
    """
    Calculate the count of women who attended training.

    Parameters:
    initial_attendee_1, initial_attendee_2, initial_attendee_3: Attendance status for initially listed individuals.
    gender_initial_1, gender_initial_2, gender_initial_3: Gender codes for initially listed individuals,
    where '2' is woman.
    additional_attendee_1, additional_attendee_2, additional_attendee_3: Attendance status for additional individuals.
    gender_additional_1, gender_additional_2, gender_additional_3: Gender codes for additional individuals,
    where '2' is woman.

    Returns:
    int: The total count of women who attended training.
    """
    # Initialize the count of women and total attendees
    women_attendees_count = 0
    total_attendees_count = 0

    # Create a list of tuples for initial attendees and their gender
    initial_attendees = zip(
        [initial_attendee_1, initial_attendee_2, initial_attendee_3],
        [gender_initial_1, gender_initial_2, gender_initial_3],
    )

    # Create a list of tuples for additional attendees and their gender
    additional_attendees = zip(
        [additional_attendee_1, additional_attendee_2, additional_attendee_3],
        [gender_additional_1, gender_additional_2, gender_additional_3],
    )

    # Iterate through the initial attendees
    for attendee, gender in initial_attendees:
        # print('Initials: ', attendee, gender)
        if str(attendee) == "1":
            total_attendees_count += 1
            if (
                str(gender) == "2"
            ):  # Check if attendee is '2' (attended) and gender is '2' (woman)
                women_attendees_count += 1

    # Iterate through the additional attendees
    for attendee, gender in additional_attendees:
        # print('Additional: ', attendee, gender)
        if str(gender) == "1" or str(gender) == "2":
            total_attendees_count += 1
            if str(gender) == "2":  # Check if gender is '2' (woman)
                women_attendees_count += 1

    if total_attendees_count > 0:
        pct = float(women_attendees_count / total_attendees_count)
    else:
        pct = 0

    # print(women_attendees_count, total_attendees_count, pct)
    # print(women_attendees_count)
    # print(total_attendees_count)
    # print(pct)
    # print('total: ' + str(total_attendees_count))
    return pct


def check_soil_assessment_use(response):
    """Check if the producer selected choice '4'"""
    # print(1 if str(response) == '1' else 0)
    return str(response) == "1"


def calculate_soil_practices_number(*practices):
    """Add together the number of practices adopted from specified questions."""
    return sum(1 for practice in practices if str(practice) == "1")


def calculate_num_practices_for_pest_management(*practices):
    total = sum(1 for practice in practices if str(practice) == "1")
    return total >= 3


def calculate_vulnerable_groups_excluded_from_pesticide_use(*practices):
    # return all(element == '1' for element in practices) ## DEPRECATED 20240822 (PM & AE)

    # Check if all elements from options 1 to 5 are '1'
    options_1_to_5_check = all(element == "1" for element in practices[:5])

    # Check if the option 6 is '1'
    option_6_check = practices[5] == "1"

    # Check if option 7 is '1'
    option_7_check = practices[6] == "1"

    # Return True if any of the above conditions are met
    return (options_1_to_5_check and not option_6_check) or option_7_check


def calculate_provides_all_protective_gear(*practices, no_apply=None):
    print(practices, no_apply)
    val = all(str(element) == "1" for element in practices) or str(no_apply) == "1"
    print("protective-gear: " + str(val))
    return val


def list_soil_conservation_practices(*practices):
    # TODO: Needs to be reviewed after conversation with AE/Papa
    """List the names of all soil practices adopted (answered with a "1")."""
    practice_names = [
        "Practice 12_1_1",
        "Practice 12_1_2",
        "Practice 12_1_3",
        "Practice 12_1_4",
        "Practice 12_1_5",
        "Practice 12_1_6",
        "Practice 12_2_2",
        "Practice 12_2_4",
        "Practice 12_3_1",
        "Practice 12_3_2",
        "Practice 12_3_3",
    ]
    adopted_practices = [
        name
        for practice, name in zip(practices, practice_names)
        if practice.strip() == "1"
    ]
    return adopted_practices


def check_deforestation_risk_none(land_change, response1, response2, other):
    """
    Check if the producer sees no deforestation risk (answered "2" or "3" to q4_1).
    Otherwise, check answers for q4_3_1 and q4_3_2.
    """
    if str(land_change) == "2" or str(land_change) == "3":
        return True
    return str(response1) == "1" or str(response2) == "1" or str(other) == "1"


def check_locally_adapted_varieties(
    response, varieties=None, v11=None, v12=None, v13=None, v14=None
):
    adapted = None
    if (
        varieties == ""
    ):  # if they have answered the varieties question, means they are using locally adapted varieties
        adapted = 1
    if response == "1":
        adapted = 1
    if response == "2":
        adapted = 0.75
    if response == "3":
        adapted = 0.50
    if response == "4":
        adapted = 0.25
    if response == "5":
        adapted = 0.10
    if response == "6":
        adapted = 0
    if response == "99":
        adapted = 0
    if v11 == "1" or v12 == "1" or v13 == "1" or v14 == "1":
        adapted = 1
    return adapted


def check_coffee_trees_planted(response):
    """Check if the producer reported planting coffee trees (any number > 0)."""
    try:
        return int(response) > 0
    except ValueError:
        return False


def check_other_trees_planted(*responses):
    """Check if the producer reported planting any other types of trees."""
    return any(float(response) > 0 for response in responses)


def count_water_conservation_measures(q12_2_1, q12_2_3, q12_2_5):
    """Count the number of times '1' appears in the specified water conservation questions."""
    return sum(1 for response in [q12_2_1, q12_2_3, q12_2_5] if response == "1")


def check_water_contamination_risk(q13_1, q13_2, q13_3):
    """Check if there is no risk of water contamination."""
    no_risk_answers = ["2"]
    risk = all(response not in no_risk_answers for response in [q13_1, q13_2, q13_3])
    return risk


def check_organic_waste_recycling(q14_1, q14_2):
    """Check if the producer recycles organic waste."""
    recycling_options = ["1", "5"]
    return q14_1 in recycling_options or q14_2 in recycling_options


def check_energy_use_coffee_production(q23_1):
    """Check if energy is used in coffee production."""
    return str(q23_1) == "1"


def check_renewable_energy_use(q23_1, q23_2):
    """Check if renewable energy is used."""
    v = False
    if q23_1 == "1":
        if q23_2 == "1" or q23_2 == "2" or q23_2 == "3":
            v = True
    else:
        v = None
    return v


def check_income_coffee_main(value):
    return str(value) == "1"


def check_market_price_awareness(info1, info2, info3, info4):
    return info1 == "1" or info2 == "1" or info3 == "1" or info4 == "1"


def check_price_understanding_always(value):
    return str(value) == "1"


def check_record_keeping(value):
    return str(value) == "1"


def check_affordable_loan_access(source1, source2):
    return source1 == "1" or source2 == "1"


def check_coffee_farming_beneficial(value):
    return str(value) in ["1", "2"]


def check_coffee_farming_prospects_good(value):
    return str(value) in ["1", "2"]


def check_children_coffee_farming_happy(value):
    return str(value) in ["1", "2"]


def check_coffee_farming_continuation_intention(value):
    return str(value) in ["4", "5"]


def check_certification_joined(value):
    return str(value) == "1"


def check_certification_left(value):
    return str(value) == "2"


def check_uses_no_banned_pesticides(value):
    return str(value) == "1"


def check_reports_higher_income(param, sold_most_production):
    if sold_most_production == "1" or sold_most_production is None:
        # print(param, sold_most_production, 1 if str(param) in ['1', '2'] else 0)
        return str(param) in ["1", "2"]
    # print(param, sold_most_production, 0)
    return False


def check_reports_higher_fertilizer_costs(param):
    return str(param) in ["1", "2"]


def check_organic_waste_recycling(param1, param2):
    return str(param1) in ["1", "5", "7"] or str(param2) in ["1", "5", "7"]


def calculate_crop_area_ha(area, unit="Hectares"):
    if area is not None:
        area = float(area)
    else:
        area = 0
    area_ha = None
    if unit == "Hectares":
        area_ha = area
    if unit == "Manzanas":
        area_ha = area * 0.7044
    if unit == "Acres":
        area_ha = area * 0.4046
    if unit == "Cuerdas":
        area_ha = area * 0.3930
    if unit == "Square Meters":
        area_ha = area * 0.0001
    if unit == "Tareas":
        area_ha = area * 0.0628
    return area_ha


def calculate_yield_kgs_ha(
    yields, weight_units, crop_stages, area, area_unit="Hectares"
):
    """
    Function to calculate yield in kilograms GBE for given yields, weight units, and crop stages.

    Parameters:
    yields: List or tuple of yields.
    weight_units: Corresponding list or tuple of weight units for each yield.
        Options are:
        - Kgs
        - Lbs
        - Qqs
        - Sacks 60 kgs
        - Cargas
    crop_stages: Corresponding list or tuple of crop stages for each yield.
        Options are:
        - Dry parchment
        - Wet parchment
        - Fresh cherry
        - Dry cherry
        - Green bean

    Returns:
    List: Calculated yields in kilograms GBE.

    Example usage:
    yields = [100, 200, 300]
    weight_units = ['Kgs', 'Lbs', 'Qqs']
    crop_stages = ['Dry Parchment', 'Wet parchment', 'Fresh cherry']

    calculated_yields = calculate_yield_kgs_gbe(yields, weight_units, crop_stages)
    print(calculated_yields)
    """
    crop_area_ha = calculate_crop_area_ha(area, area_unit)
    total_yields = 0
    for yield_val, weight_unit, crop_stage in zip(yields, weight_units, crop_stages):
        # Convert yields to Kgs
        yield_kgs = None
        if yield_val is not None:
            yield_val = float(yield_val)
        else:
            yield_val = 0
        if weight_unit == "Kgs":
            yield_kgs = yield_val
        if weight_unit == "Lbs":
            yield_kgs = yield_val / 2.20462
        if weight_unit == "Qqs":
            yield_kgs = yield_val * 45.35
        if weight_unit == "Cargas":
            yield_kgs = yield_val * 140
        if weight_unit == "Sacks 45 Kgs":
            yield_kgs = yield_val * 45
        if weight_unit == "Sacks 60 Kgs":
            yield_kgs = yield_val * 60
        # Convert yields to green bean
        yield_kgs_gbe = None
        if yield_kgs is not None:
            if crop_stage == "Dry parchment":
                yield_kgs_gbe = yield_kgs * 0.8
            if crop_stage == "Wet parchment":
                yield_kgs_gbe = yield_kgs * 0.5085
            if crop_stage == "Fresh cherry":
                yield_kgs_gbe = yield_kgs * 0.17
            if crop_stage == "Dry cherry":
                yield_kgs_gbe = yield_kgs * 0.5
            if crop_stage == "Green bean":
                yield_kgs_gbe = yield_kgs

        # print("%s, %s, %s, %s, %s" % (weight_unit, crop_stage, yield_val, yield_kgs, yield_kgs_gbe))

        # Add yield_kgs_gbe to the total
        if yield_kgs_gbe is not None:
            total_yields += yield_kgs_gbe

    if crop_area_ha > 0:
        # print(",,,,,Hectares:, %s, Total:, %s" % (crop_area_ha, float(total_yields) / float(crop_area_ha)))
        # print(total_yields / crop_area_ha)
        return total_yields / crop_area_ha
    return None


def homogenize_weight_unit(survey_id, selected_option):
    """
    Function to convert selected unit IDs to text.
    Todo: Move this parametrization to the database
    """
    unit = None
    if survey_id in ["37", "38", "39", "40", "41"]:  # FB Nicaragua
        unit = (
            "Lbs"
            if selected_option == "1"
            else (
                "Qqs"
                if selected_option == "2"
                else "Cargas" if selected_option == "3" else None
            )
        )
    if survey_id in ["42", "43", "44", "45", "46", "100"]:  # FB Brazil
        unit = (
            "Lbs"
            if selected_option == "1"
            else (
                "Kgs"
                if selected_option == "2"
                else (
                    "Sacks 60 Kgs"
                    if selected_option == "3"
                    else "Sacks 45 Kgs" if selected_option == "99" else None
                )
            )
        )
    return unit


# Ensure a survey ID was provided
if len(sys.argv) < 2:
    print("Usage: metrics.py <survey_id>")
    sys.exit(1)

survey_id_of_interest = sys.argv[1]

# Database connection parameters
if os.environ.get("PORT") is None:
    load_dotenv()
db_host = os.environ["DB_HOST"]
db_port = os.environ["DB_PORT"]
db_name = os.environ["DB_NAME"]
db_user = os.environ["DB_USER"]
db_password = os.environ["DB_PASSWORD"]

db_params = {
    "database": db_name,
    "user": db_user,
    "password": db_password,
    "host": db_host,
    "port": 5432,
}

# Context manager for database connection
with psycopg2.connect(**db_params) as conn, conn.cursor() as cur:
    response_query = (
        "SELECT r.instanceid, s.default_area_unit, s. default_weight_unit, questionid, responsevalue\n"
        "FROM public.externalsurveyresponse r\n"
        "JOIN public.externalsurveyinstance i ON i.instanceid = r.instanceid\n"
        "JOIN public.externalsurvey s ON s.surveyid = i.surveyid\n"
        "WHERE i.surveyid = %s;"
    )
    response_df = pd.read_sql_query(
        response_query, conn, params=[survey_id_of_interest]
    )
    pivoted_df = response_df.pivot(
        index="instanceid", columns="questionid", values="responsevalue"
    )

    default_area_unit = None
    default_weight_unit = None
    if not response_df.empty:
        default_area_unit = response_df["default_area_unit"].iloc[0] or "Hectares"
        default_weight_unit = response_df["default_weight_unit"].iloc[0] or "Kilograms"

    print(pivoted_df)

    """
    metrics_df = pivoted_df.apply(
        lambda row: pd.Series({
            'training_women_attendees_count': calculate_training_women_attendees_count(
                row.get('q19_11_1', None), row.get('q19_11_2', None), row.get('q19_11_3', None),
                row.get('q3_1b_1', None), row.get('q3_1b_2', None), row.get('q3_1b_3', None),
                row.get('q19_12a_1', None), row.get('q19_12a_2', None), row.get('q19_12a_3', None),
                row.get('q19_12b_1', None), row.get('q19_12b_2', None), row.get('q19_12b_3', None)),
        }), axis=1)
    """
    # WESTROCK METRICS
    # Use apply() to create a DataFrame of metrics
    """
    metrics_df = pivoted_df.apply(
        lambda row: pd.Series({
            'quality_life_good': check_quality_of_life(row['q22_1']),
            'food_security_zero_days': check_food_security(row.get('q21_1')),
            'safe_water_access': check_safe_water_access(row.get('q18_1')),
            'farm_injury_free': check_farm_injury_free(row['q20_1']),
            'affordable_medical_access': check_affordable_medical_access(row['q20_3']),
            'medical_services_access': check_medical_services_access(row['q20_2']),
            'women_decision_maker': check_women_decision_maker(row['q3_1b_1']),
            'training_women_attendees_count': calculate_training_women_attendees_count(
                row.get('q19_11_1', None), row.get('q19_11_2', None), row.get('q19_11_3', None),
                row.get('q3_1b_1', None), row.get('q3_1b_2', None), row.get('q3_1b_3', None),
                row.get('q19_12a_1', None), row.get('q19_12a_2', None), row.get('q19_12a_3', None),
                row.get('q19_12b_1', None), row.get('q19_12b_2', None), row.get('q19_12b_3', None)),
            'soil_assessment_use': check_soil_assessment_use(row.get('q15_3_4', None)),
            'soil_practices_number': calculate_soil_practices_number(row.get('q12_1_1'), row.get('q12_1_2'),
                                                                     row.get('q12_1_3'), row.get('q12_1_4'),
                                                                     row.get('q12_1_5'), row.get('q12_1_6'),
                                                                     row.get('q12_2_2'), row.get('q12_2_4'),
                                                                     row.get('q12_3_1'), row.get('q12_3_2'),
                                                                     row.get('q12_3_3')),
            'deforestation_risk_none': check_deforestation_risk_none(row.get('q4_1'), row.get('q4_3_1'),
                                                                     row.get('q4_3_2'),row.get('q4_3_99')),
            'coffee_trees_planted': check_coffee_trees_planted(row.get('q5_2a')),
            'other_trees_planted': check_other_trees_planted(row.get('q5_2b'), row.get('q5_2c'), row.get('q5_2d')),
            'locally_adapted_varieties': check_locally_adapted_varieties(row.get('q6_4'), row.get('q6_5'),
                                                                         row.get('q6_4_11'), row.get('q6_4_12'),
                                                                         row.get('q6_4_13'), row.get('q6_4_14')),
            'water_conservation_measures': count_water_conservation_measures(row.get('q12_2_1'), row.get('q12_2_3'),
                                                                             row.get('q12_2_5')),
            'water_contamination_risk': check_water_contamination_risk(row.get('q13_1'), row.get('q13_2'),
                                                                       row.get('q13_3')),
            'organic_waste_recycling': check_organic_waste_recycling(row.get('q14_1'), row.get('q14_2')),
            'energy_use_coffee_production': check_energy_use_coffee_production(row.get('q23_1')),
            'renewable_energy_use': check_renewable_energy_use(row.get('q23_1'), row.get('q23_2')),
            'income_coffee_main': check_income_coffee_main(row.get('q7_7')),
            'market_price_awareness': check_market_price_awareness(row.get('q8_2_1'), row.get('q8_2_2'),
                                                                   row.get('q8_2_3'), row.get('q8_2_4')),
            'price_understanding_always': check_price_understanding_always(row.get('q8_1')),
            'record_keeping': check_record_keeping(row.get('q10_1')),
            'affordable_loan_access': check_affordable_loan_access(row.get('q11_1_4'), row.get('q11_1_5')),
            'coffee_farming_beneficial': check_coffee_farming_beneficial(row.get('q22_2')),
            'coffee_farming_prospects_good': check_coffee_farming_prospects_good(row.get('q22_3')),
            'children_coffee_farming_happy': check_children_coffee_farming_happy(row.get('q22_4')),
            'coffee_farming_continuation_intention': check_coffee_farming_continuation_intention(row.get('q22_5')),
            'certification_joined': check_certification_joined(row.get('q2_1')),
            'certification_left': check_certification_left(row.get('q2_1'))
        }), axis=1)
    """
    # FARMER BROTHERS METRICS
    # Use apply() to create a DataFrame of metrics
    """
    metrics_df = pivoted_df.apply(
        lambda row: pd.Series(dict(
                                   income_coffee_main=check_income_coffee_main(row.get('q5_13')),
                                   price_understanding_always=check_price_understanding_always(row.get('q6_1')),
                                   reports_higher_income=check_reports_higher_income(row.get('q5_9'), row.get('q5_15'))
            if survey_id_of_interest in ['42', '43', '44', '45', '46'] else
            check_reports_higher_income(row.get('q5_9'), None),
                                   reports_higher_fertilizer_costs=check_reports_higher_fertilizer_costs(row.get('q13_5')),

                                   yield_kgs_ha=calculate_yield_kgs_ha([row.get('q5_14')],
                                                                       [homogenize_weight_unit(survey_id_of_interest,'3')],
                                                                       ['Green bean'],
                                                                       row.get('q3_2'), default_area_unit
                                                                       ) if survey_id_of_interest in ['42', '43', '44', '45', '46', '100'] else calculate_yield_kgs_ha([row.get('q5_4a_1'), row.get('q5_4b_1'),
                                                                            row.get('q5_4c_1'), row.get('q5_4d_1'),
                                                                            row.get('q5_4e_1'), row.get('q5_4f_1')],
                                                                           [homogenize_weight_unit(survey_id_of_interest, row.get('q5_4a_2')),
                                                                            homogenize_weight_unit(survey_id_of_interest, row.get('q5_4b_2')),
                                                                            homogenize_weight_unit(survey_id_of_interest, row.get('q5_4c_2')),
                                                                            homogenize_weight_unit(survey_id_of_interest, row.get('q5_4d_2')),
                                                                            homogenize_weight_unit(survey_id_of_interest, row.get('q5_4e_2')),
                                                                            homogenize_weight_unit(survey_id_of_interest, row.get('q5_4f_2'))],
                                                                           ['Fresh cherry', 'Dry parchment',
                                                                            'Wet parchment', 'Green bean',
                                                                            'Green bean' if row.get('q5_3other') == 'Limpo' else 'Fresh cherry', 'Fresh cherry'],
                                                                           row.get('q3_2'), default_area_unit)
       )), axis=1)
    """

    metrics_df = pivoted_df.apply(
        lambda row: pd.Series(
            dict(
                food_security_zero_days=check_food_security(row.get("q17_1")),
                safe_water_access=check_safe_water_access(row.get("q2_9")),
                soil_assessment_use=check_soil_assessment_use(row.get("q12_3_4", None)),
                soil_practices_number=calculate_soil_practices_number(
                    row.get("q9_1_1"),
                    row.get("q9_1_2"),
                    row.get("q9_1_3"),
                    row.get("q9_1_4"),
                    row.get("q9_1_5"),
                    row.get("q9_1_6"),
                    row.get("q9_2_2"),
                    row.get("q9_2_4"),
                    row.get("q9_3_1"),
                    row.get("q9_3_2"),
                    row.get("q9_3_3"),
                ),
                deforestation_risk_none=check_deforestation_risk_none(
                    row.get("q4_1"),
                    row.get("q4_3_1"),
                    row.get("q4_3_2"),
                    row.get("q4_3_99"),
                ),
                locally_adapted_varieties=check_locally_adapted_varieties(
                    row.get("q8_1")
                ),
                water_conservation_measures=count_water_conservation_measures(
                    row.get("q9_2_1"), row.get("q9_2_3"), row.get("q9_2_5")
                ),
                water_contamination_risk=check_water_contamination_risk(
                    row.get("q10_1"), row.get("q10_2"), row.get("q10_3")
                ),
                income_coffee_main=check_income_coffee_main(row.get("q5_13")),
                price_understanding_always=check_price_understanding_always(
                    row.get("q6_1")
                ),
                record_keeping=check_record_keeping(row.get("q7_1")),
                num_practices_for_pest_management=calculate_num_practices_for_pest_management(
                    row.get("q13_1_1"),
                    row.get("q13_1_2"),
                    row.get("q13_1_3"),
                    row.get("q13_1_4"),
                    row.get("q13_1_5"),
                    row.get("q13_1_6"),
                ),
                uses_no_banned_pesticides=(
                    check_uses_no_banned_pesticides(row.get("q13_2_19"))
                    if survey_id_of_interest in ["37", "38", "39", "40", "41"]
                    else check_uses_no_banned_pesticides(row.get("q13_2_22"))
                ),
                vulnerable_groups_excluded_from_pesticide_use=calculate_vulnerable_groups_excluded_from_pesticide_use(
                    row.get("q13_3_1"),
                    row.get("q13_3_2"),
                    row.get("q13_3_3"),
                    row.get("q13_3_4"),
                    row.get("q13_3_5"),
                    row.get("q13_3_6"),
                    row.get("q13_3_7"),
                ),
                provides_all_protective_gear=(
                    calculate_provides_all_protective_gear(
                        row.get("q13_4_2"),
                        row.get("q13_4_3"),
                        row.get("q13_4_4"),
                        row.get("q13_4_5"),
                        no_apply=row.get("q13_4_7"),
                    )
                    if survey_id_of_interest in ["37", "38", "39", "40", "41"]
                    else calculate_provides_all_protective_gear(
                        row.get("q13_4_2"),
                        row.get("q13_4_3"),
                        row.get("q13_4_4"),
                        row.get("q13_4_5"),
                        row.get("q13_4_6"),
                        no_apply=row.get("q13_4_8"),
                    )
                ),
                reports_higher_income=(
                    check_reports_higher_income(row.get("q5_9"), row.get("q5_15"))
                    if survey_id_of_interest in ["42", "43", "44", "45", "46"]
                    else check_reports_higher_income(row.get("q5_9"), None)
                ),
                reports_higher_fertilizer_costs=check_reports_higher_fertilizer_costs(
                    row.get("q13_5")
                ),
                organic_waste_recycling=check_organic_waste_recycling(
                    row.get("q11_1"), row.get("q11_2")
                ),
                crop_area_ha=calculate_crop_area_ha(row.get("q3_2"), default_area_unit),
                yield_kgs_ha=(
                    calculate_yield_kgs_ha(
                        [row.get("q5_14")],
                        [homogenize_weight_unit(survey_id_of_interest, "3")],
                        ["Green bean"],
                        row.get("q3_2"),
                        default_area_unit,
                    )
                    if survey_id_of_interest in ["42", "43", "44", "45", "46"]
                    else calculate_yield_kgs_ha(
                        [
                            row.get("q5_4a_1"),
                            row.get("q5_4b_1"),
                            row.get("q5_4c_1"),
                            row.get("q5_4d_1"),
                            row.get("q5_4e_1"),
                            row.get("q5_4f_1"),
                        ],
                        [
                            homogenize_weight_unit(
                                survey_id_of_interest, row.get("q5_4a_2")
                            ),
                            homogenize_weight_unit(
                                survey_id_of_interest, row.get("q5_4b_2")
                            ),
                            homogenize_weight_unit(
                                survey_id_of_interest, row.get("q5_4c_2")
                            ),
                            homogenize_weight_unit(
                                survey_id_of_interest, row.get("q5_4d_2")
                            ),
                            homogenize_weight_unit(
                                survey_id_of_interest, row.get("q5_4e_2")
                            ),
                            homogenize_weight_unit(
                                survey_id_of_interest, row.get("q5_4f_2")
                            ),
                        ],
                        [
                            "Fresh cherry",
                            "Dry parchment",
                            "Wet parchment",
                            "Green bean",
                            (
                                "Green bean"
                                if row.get("q5_3other") == "Limpo"
                                else "Fresh cherry"
                            ),
                            "Fresh cherry",
                        ],
                        row.get("q3_2"),
                        default_area_unit,
                    )
                ),
            )
        ),
        axis=1,
    )

    # Save calculated metric into the database
    insert_query = "INSERT INTO surveymetrics (surveyid, instanceid, metricid, metricvalue) VALUES (%s, %s, %s, %s)"

    """
    for instance_id, metrics in metrics_df.iterrows():
        for metric_id, metric_value in metrics.items():
            #print(instance_id, metric_id, metric_value)
            #cur.execute(insert_query, (survey_id_of_interest, instance_id, metric_id, metric_value))
    #conn.commit()
    """

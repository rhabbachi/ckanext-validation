# -*- coding: utf-8 -*-
from goodtables import validate
from ckanext.validation.custom_checks import setup_custom_goodtables

setup_custom_goodtables()


# Validate
def test_check_custom_constraint(log):
    source = [
        ['row', 'salary', 'bonus'],
        [2, 1000, 200],
        [3, 2500, 500],
        [4, 1300, 500],
        [5, 5000, 1000],
        [6, 6000, 2000]
    ]
    report = validate(source, checks=[
        {'custom-constraint': {'constraint': 'salary > bonus * 4'}},
    ])
    assert log(report) == [
        (1, 4, None, 'custom-constraint'),
        (1, 6, None, 'custom-constraint'),
    ]


def test_check_custom_constraint_for_missing_data(log):

    source = [
        ['row', 'salary', 'bonus'],
        [1, None, 500],
        [2, 5000],
        [3]
    ]
    report = validate(source, checks=[
        {'custom-constraint': {'constraint': 'salary > bonus * 4'}},
    ])
    assert log(report) == []


def test_check_custom_constraint_incorrect_constraint(log):
    source = [
        ['row', 'name'],
        [2, 'Alex'],
    ]
    report = validate(source, checks=[
        {'custom-constraint': {'constraint': 'vars()'}},
        {'custom-constraint': {'constraint': 'import(os)'}}
    ])
    assert log(report) == [
        (1, 2, None, 'custom-constraint'),
        (1, 2, None, 'custom-constraint'),
    ]

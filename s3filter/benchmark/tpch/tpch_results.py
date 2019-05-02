from datetime import date

q1_sf1_expected_result = None

# q14_sf1_expected_result = 15.09012
q14_sf1_expected_result = 16.63567
q17_sf1_expected_result = 372414.28999
q19_sf1_expected_result = 3468861.09700

q14_sf10_expected_result = 16.59267
q17_sf10_expected_result = 3355574.50571
q19_sf10_expected_result = 34370139.02529

q3_sf1_expected_result = [
    ['492164', date(1995, 2, 19), 0, 426168.0874],
    ['4617957', date(1995, 1, 28), 0, 390773.246],
    ['2020610', date(1995, 2, 5), 0, 385595.0357],
    ['4487361', date(1995, 2, 22), 0, 381485.733],
    ['1190215', date(1995, 2, 26), 0, 379378.5947],
    ['2435712', date(1995, 2, 26), 0, 378673.0558],
    ['5455494', date(1995, 2, 21), 0, 375787.7068],
    ['2628192', date(1995, 2, 22), 0, 373133.3094],
    ['4738049', date(1995, 2, 19), 0, 372498.1426],
    ['3978503', date(1995, 2, 9), 0, 370171.7512]
]


# q3_sf1_testing
q3_sf1_testing_params = {
    'customer_filter': " cast(c_custkey as int) in ("
                       "55126, 61099, 72703, 140578,78536,112090,116101,"
                       "15377,46499,108928,64732,137560,33862,28244,57469"
                       ") ",
    'order_filter': " cast(o_custkey as int) in ("
                    "55126, 61099, 72703, 140578,78536,112090,116101,"
                    "15377,46499,108928,64732,137560,33862,28244,57469"
                    ") ",
    'lineitem_filter': " cast(l_orderkey as int) in ("
                       "577, 1281, 1637, 2053, 2114, 3430, 3814, 6791, "
                       "7426, 7744, 7840, 8133, 8192, 8195, 9696"
                       ") "
}

q3_sf1_testing_expected_result = [
    ["1637", date(1995, 2, 8), 0, 268170.6408],
    ["9696", date(1995, 2, 20), 0, 252014.5497],
    ["8133", date(1995, 2, 27), 0, 188626.4181],
    ["2053", date(1995, 2, 7), 0, 183820.5208],
    ["8195", date(1995, 1, 7), 0, 174941.6546],
    ["7840", date(1995, 1, 9), 0, 166329.4848],
    ["3814", date(1995, 2, 22), 0, 153733.9603],
    ["1281", date(1994, 12, 11), 0, 86647.2392],
    ["6791", date(1995, 2, 2), 0, 78354.5360],
    ["8192", date(1995, 1, 7), 0, 71899.1910],
]

q3_sf10_testing_expected_result = [
    ['52974151', date(1995, 2, 5), 0, 480148.7595],
    ['4791171', date(1995, 2, 23), 0, 440715.2185],
    ['46678469', date(1995, 1, 27), 0, 439855.325],
    ['59393639', date(1995, 2, 12), 0, 426036.0662],
    ['22810436', date(1995, 1, 2), 0, 423231.969],
    ['34629446', date(1995, 2, 6), 0, 419335.7392],
    ['12868901', date(1995, 2, 19), 0, 418887.005],
    ['15473856', date(1995, 2, 15), 0, 418709.8204],
    ['33688902', date(1995, 2, 18), 0, 418285.4882],
    ['22756039', date(1995, 2, 23), 0, 417043.8712],
]
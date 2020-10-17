class DataChecks:
    dq_checks_sql=([{'table':'users','check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
                        {'table':'songs','check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
                        {'table':'time','check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0},
                        {'table':'artists','check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
                        {'table':'songplays','check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0}])
        
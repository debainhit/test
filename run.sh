#
#!/usr/bin/env bash
/usr/local/hadoop-2.4.1/bin/hadoop jar /usr/local/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar \
    -Dstream.non.zero.exit.is.failure=false \
    -Dmapred.map.tasks.speculative.execution=false \
    -Dmapred.reduce.tasks.speculative.execution=false \
    -Dmapred.reduce.tasks=50 \
    -input /user/drill/almark/20170719_usage_dialer_new \
    -output /user/drill/almark/path_login_20170719 \
    -file 'mapper.py' \
    -mapper 'python mapper.py' \
    -reducer 'cat'

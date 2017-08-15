#!/bin/bash


# responsible for running cron tasks
# manages creating logs for tasks by redirecting std_out/err
#
# input args
#   branch
#   cron task keyword


branch=$1
cron=$2

src="${HOME}"/active/"$branch"

timestamp=$(date +%Y%m%d)

# make sure log directories exist
# mkdir -p "$src"/log/{update_repos,cleanup_repos,db_updates,msr}

# cron tasks do not inherit pythonpath from user,
# so it needs to be specified here
export PYTHONPATH="${PYTHONPATH}:/usr/local/amd64/seoul/gcc/python-2.7.8/lib/python2.7/site-packages/:/sciclone/aiddata10/REU/py_libs/lib/python2.7/site-packages"

case $cron in
    "update_repos")         mkdir -p "$src"/log/update_repos
                            bash "$src"/asdf/src/tasks/update_repos.sh "$branch" 2>&1 | tee 1>>"$src"/log/update_repos/"$timestamp".update_repos.log
                            exit 0;;

    "cleanup_repos")        mkdir -p "$src"/log/cleanup_repos
                            bash "$src"/asdf/src/tasks/cleanup_repos.sh "$branch" 2>&1 | tee 1>>"$src"/log/cleanup_repos/"$timestamp".cleanup_repos.log
                            exit 0;;

    "build_update_trackers_job")    mkdir -p "$src"/log/update_trackers
                                    bash "$src"/asdf/src/tasks/build_jobs.sh "$branch" "$timestamp" update_trackers 2>&1 | tee 1>>"$src"/log/update_trackers/"$timestamp".update_trackers.log
                                    exit 0;;

    "build_update_extract_job")     mkdir -p "$src"/log/update_extract
                                    bash "$src"/asdf/src/tasks/build_jobs.sh "$branch" "$timestamp" update_extract 2>&1 | tee 1>>"$src"/log/update_extract/"$timestamp".update_extract.log
                                    exit 0;;

    "build_update_msr_job")         mkdir -p "$src"/log/update_msr
                                    bash "$src"/asdf/src/tasks/build_jobs.sh "$branch" "$timestamp" update_msr 2>&1 | tee 1>>"$src"/log/update_msr/"$timestamp".update_msr.log
                                    exit 0;;

    "build_det_job")        mkdir -p "$src"/log/det
                            bash "$src"/asdf/src/tasks/build_jobs.sh "$branch" "$timestamp" det 2>&1 | tee 1>>"$src"/log/det/"$timestamp".det.log
                            exit 0;;

    "build_msr_job")        mkdir -p "$src"/log/msr
                            bash "$src"/asdf/src/tasks/build_msr_job.sh "$branch" "$timestamp" 2>&1 | tee 1>>"$src"/log/msr/"$timestamp".msr.log
                            exit 0;;

    "build_extract_job")    mkdir -p "$src"/log/extract
                            bash "$src"/asdf/src/tasks/build_extract_job.sh "$branch" "$timestamp" 2>&1 | tee 1>>"$src"/log/extract/"$timestamp".extract.log
                            exit 0;;


    *)                      echo "Invalid cron.";
                            exit 1 ;;
esac


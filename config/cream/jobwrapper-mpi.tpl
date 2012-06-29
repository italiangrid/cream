
###################################################################
#                           Procedures
###################################################################

do_transfer() # 1 - command, 2 - source, 3 - dest, 4 - std err, 5 - exit code file
{
  eval $1 "$2" "$3" 2>"$4"
  echo $? > "$5"
}

do_exit() {
  stat=$1
  echo "jw exit status = ${stat}"
  echo $2 1>&2

  if [ $__create_subdir -eq 1 ]; then
    cd ..
    rm -rf ${newdir}
  fi

  exit $stat
}

log_and_exit() {
  if [ -n "$3" ]; then
    notifyToCream "JobStatus=4; Reason=\"$2\"; ExitCode=$3;"
    __local_exit_code=$3
  else
    notifyToCream "JobStatus=4; Reason=\"$2\";"
    __local_exit_code=0
  fi

  if [ -n "$GLITE_WMS_SEQUENCE_CODE" -a -f "$LB_LOGEVENT" ]; then
    GLITE_WMS_SEQUENCE_CODE=`$LB_LOGEVENT \
                               --jobid="$GLITE_WMS_JOBID" \
                               --source=LRMS \
                               --sequence="$GLITE_WMS_SEQUENCE_CODE" \
                               --event="Done" \
                               --reason="$2" \
                               --status_code=FAILED \
                               --exit_code=${__local_exit_code} \
                             || echo $GLITE_WMS_SEQUENCE_CODE`
  fi

  do_exit $1 "$2"
}

retry_copy() # 1 - command, 2 - source, 3 - dest
{
  count=0
  succeded=1
  sleep_time=0
  while [ $count -le ${__copy_retry_count} -a $succeded -ne 0 ];
  do
    time_left=`grid-proxy-info -timeleft 2>/dev/null || echo 0`;
    if [ $time_left -lt $sleep_time ]; then
      retry_error_message="proxy expired"
      return 1
    fi
    sleep $sleep_time
    if [ $sleep_time -eq 0 ]; then
      sleep_time=${__copy_retry_first_wait}
    else
      sleep_time=`expr $sleep_time \* 2`
    fi
    transfer_stderr=`mktemp -q std_err.XXXXXXXXXX`
    if [ ! -f "$transfer_stderr" ]; then
      transfer_stderr="/dev/null"
    fi
    transfer_exitcode=`mktemp -q tr_exit_code.XXXXXXXXXX`
    if [ ! -f "$transfer_exitcode" ]; then
      transfer_exitcode="/dev/null"
    fi
    do_transfer $1 "$2" "$3" "$transfer_stderr" "$transfer_exitcode"&
    transfer_watchdog_pid=$!
    transfer_timeout=3600
    while [ $transfer_timeout -gt 0 ];
    do
      if [ -z `ps -p $transfer_watchdog_pid -o pid=` ]; then
        break;
      fi
      sleep 1
      let "transfer_timeout--"
    done
    if [ $transfer_timeout -le 0 ]; then
      kill -9 $transfer_watchdog_pid
      retry_error_message="Hanging transfer"
      succeded=1
    else
      succeded=`cat $transfer_exitcode 2>/dev/null`
      if [ -z $succeded ]; then
        retry_error_message="Cannot retrieve return value for transfer"
        succeded=1
      fi
    fi
    rm -f "$transfer_stderr" "$transfer_exitcode"
    count=`expr $count + 1`
  done
  return ${succeded}
}

function send_partial_file {
  local TRIGGERFILE=$1
  local DESTURL=$2
  local POLL_INTERVAL=$3
  local FILESUFFIX=partialtrf
  local GLOBUS_RETURN_CODE
  local SLICENAME
  local LISTFILE=`pwd`/listfile.$$
  local LAST_CYCLE=""
  local SLEEP_PID
  local MD5
  local OLDSIZE
  local NEWSIZE
  local COUNTER

  while [ -z "$LAST_CYCLE" ] ; do

    sleep $POLL_INTERVAL & SLEEP_PID=$!
    trap 'LAST_CYCLE="y"; kill -ALRM $SLEEP_PID >/dev/null 2>&1' USR2
    wait $SLEEP_PID >/dev/null 2>&1

    if [ "${TRIGGERFILE:0:9}" == "gsiftp://" ]; then
      retry_copy ${globus_transfer_cmd} ${TRIGGERFILE} file://${LISTFILE}
    elif [ "${TRIGGERFILE:0:8}" == "https://" ]; then
      retry_copy ${https_transfer_cmd} ${TRIGGERFILE} file://${LISTFILE}
    fi

    if [ "$?" -ne "0" ] ; then
      continue
    fi
    for SRCFILE in `cat $LISTFILE` ; do

      if [ "$SRCFILE" == "`basename $SRCFILE`" ] ; then
        SRCFILE=`pwd`/$SRCFILE
      fi
      if [ -f $SRCFILE ] ; then

        MD5=`expr "$(echo $SRCFILE | md5sum)" : '\([^ ]*\).*'`
        OLDSIZE="OLDSIZE_$MD5"
        COUNTER="COUNTER_$MD5"

        if [ -z "${!OLDSIZE}" ]; then eval local $OLDSIZE=0; fi
        if [ -z "${!COUNTER}" ]; then eval local $COUNTER=1; fi

        cp $SRCFILE ${SRCFILE}.${FILESUFFIX}
        NEWSIZE=`stat -c %s ${SRCFILE}.${FILESUFFIX}`
        if [ "${NEWSIZE}" -gt "${!OLDSIZE}" ] ; then
          let "DIFFSIZE = NEWSIZE - $OLDSIZE"
          SLICENAME=$SRCFILE.`date +%Y%m%d%H%M%S`_${!COUNTER}
          tail -c $DIFFSIZE ${SRCFILE}.${FILESUFFIX} > $SLICENAME
          if [ "${DESTURL:0:9}" == "gsiftp://" ]; then
            retry_copy ${globus_transfer_cmd} file://$SLICENAME ${DESTURL}/`basename $SLICENAME`
          elif [ "${DESTURL:0:8}" == "https://" ]; then
            retry_copy ${https_transfer_cmd} file://$SLICENAME ${DESTURL}/`basename $SLICENAME`
          fi
          GLOBUS_RETURN_CODE=$?
          rm ${SRCFILE}.${FILESUFFIX} $SLICENAME
          if [ "$GLOBUS_RETURN_CODE" -eq "0" ] ; then
            let "$OLDSIZE = NEWSIZE"
            let "$COUNTER += 1"
          fi
        fi
      fi
    done
  done

  if [ -f "$LISTFILE" ] ; then rm $LISTFILE ; fi
}

perl_notifier='
use Socket;

sub send_notify {
    $cream_url = "'${__logger_dest}'";die "No cream url" unless $cream_url;
    ($remote, $port) = split /:/, $cream_url, 2;die "Missing port" unless $port;

    ($prefix, $cream_id) = split /CREAM/,"'${__creamjobid}'" , 2;die "Missing cream id" unless $cream_id;

    $event_infos = $_[0];die "No infos" unless $event_infos;

    $iaddr   = inet_aton($remote) or die "no host: $remote";
    $paddr   = sockaddr_in($port, $iaddr);
    $proto   = getprotobyname("tcp");

    ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime;
    $change_time = sprintf "%4d-%02d-%02d %02d:%02d:%02d" , $year+1900 , $mon+1, $mday, $hour, $min, $sec;

    $hostname = `hostname`;
    chomp $hostname;

    socket(SOCK, PF_INET, SOCK_STREAM, $proto) or die "socket: $!";
    connect(SOCK, $paddr) or die "connect: $!";
    print SOCK "[${event_infos} ClientJobId=\"${cream_id}\"; ChangeTime=\"${change_time}\"; WorkerNode=\"${hostname}\";]\n";

    close (SOCK) or die "close: $!";
}'

function notifyToCream {
  if [ -n "${__logger_dest}" ]; then

    perl -e "${perl_notifier}"' send_notify(@ARGV);' "$1"

    return $?

  fi

  return 0
}

function notifyToLB {
  if [ -n "$GLITE_WMS_SEQUENCE_CODE" -a -f "$LB_LOGEVENT" ]; then
    GLITE_WMS_SEQUENCE_CODE=`$LB_LOGEVENT \
                               --jobid="$GLITE_WMS_JOBID" \
                               --source=LRMS \
                               --sequence="$GLITE_WMS_SEQUENCE_CODE" \
                               --event="$1" \
                               --node="$host" \
                             || echo $GLITE_WMS_SEQUENCE_CODE`
  fi
}

###################################################################
#                           Environment
###################################################################
export GLITE_WMS_LOCATION=${GLITE_LOCATION:-/opt/glite}
export GLITE_WMS_JOBID=${__gridjobid}
export GLITE_WMS_SEQUENCE_CODE="$1"
export GLITE_WMS_LOG_DESTINATION=${__ce_hostname}
shift

host=`hostname -f`

for((idx=0; idx<${#__environment[*]}; idx++)); do
  eval export ${__environment[$idx]}
done

LB_LOGEVENT=${GLITE_WMS_LOCATION}/bin/glite-lb-logevent
if [ ! -f "$LB_LOGEVENT" ]; then

  if [ -z "${EDG_WL_LOCATION}" ]; then
    export EDG_WL_LOCATION="${EDG_LOCATION:-/opt/edg}"
  fi

  LB_LOGEVENT="${EDG_WL_LOCATION}/bin/edg-wl-logev"
fi

#if [ -z "${GLITE_LOCAL_MAX_OSB_SIZE}" ]; then
#  __max_osb_size=-1
#else
#  __max_osb_size=${GLITE_LOCAL_MAX_OSB_SIZE}
#fi

notifyToCream "JobStatus=2;"

notifyToLB "Running"

if [ -n "${GLITE_LOCAL_CUSTOMIZATION_DIR}" -a -f "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_1.sh" ]; then
  . "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_1.sh"
fi

if [ $__create_subdir -eq 1 ]; then
  newdir="${__working_directory}"
  mkdir ${newdir}
  cd ${newdir}
fi

touch .tmp_file
if [ $? -ne 0 ]; then
  log_and_exit 1 "Working directory not writable"
fi

globus_transfer_cmd=globus-url-copy
https_transfer_cmd=`which htcp 2>/dev/null`
if [ $? != 0 ]; then
  https_transfer_cmd=globus-url-copy
fi

workdir="`pwd`"

# Check if grid-proxy-info is in the path
`which grid-proxy-info 1>/dev/null 2>/dev/null`
if [ $? != 0 ]; then
  log_and_exit 1 "Cannot find grid-proxy-info"
fi
           
	                                                                                                                  
trap 'do_exit 2 "Master process killed"' TERM

###################################################################
#                             ISB
###################################################################

for((idx=0; idx<${#__input_transfer_cmd[*]}; idx++)); do
  input_cmd_line="${__input_transfer_cmd[$idx]} ${__input_file_url[$idx]} file://${workdir}/${__input_file_dest[$idx]}"
  retry_copy $input_cmd_line
  if [ $? != 0 ]; then
    log_and_exit 1 "Cannot move ISB (${input_cmd_line}): ${retry_error_message}"
  fi

done

###################################################################
#                             JOB
###################################################################

if [ -e "${__executable}" ]; then
   chmod +x "${__executable}" 2> /dev/null
else
   log_and_exit 1 "${__executable} not found or unreadable"
fi

if [ -n "${__prologue}" ]; then
  if [ ! -f "${__prologue}" ]; then
    log_and_exit 1 "Prologue script does not exist"
  fi
  chmod +x "${__prologue}" 2>/dev/null
  ${__prologue} ${__prologue_arguments}
  prologue_status=$?
  if [ ${prologue_status} -ne 0 ]; then
    log_and_exit 1 "Prologue failed with error ${prologue_status}"
  fi
fi

if [ -n "${__perusal_filesdesturi}" ]; then
  send_partial_file ${__perusal_listfileuri} ${__perusal_filesdesturi} ${__perusal_timeinterval} & 
  perusal_send_pid=$!
fi

if [ -n "${__token_file}" ]; then

  gridftp_rm_cmdline=""
  for gridftp_rm_command in ${GLITE_LOCATION:-/opt/glite}/bin/glite-gridftp-rm \
                            `which glite-gridftp-rm 2>/dev/null` \
                            $EDG_LOCATION/bin/edg-gridftp-rm \
                            `which edg-gridftp-rm 2>/dev/null` ; do
    if [ -x "$gridftp_rm_command" ]; then
       gridftp_rm_cmdline="${gridftp_rm_command} ${__token_file}"
       break;
    fi
  done

  if [ ! -n "${gridftp_rm_cmdline}" ]; then
    gridftp_rm_command=`which uberftp 2>/dev/null`
    if [ -x "$gridftp_rm_command" ]; then
      majorver=`$gridftp_rm_command -version | perl -nle 'print $1 if /(\d+)+(\.)*/'`
      gridftp_option=
      if [ $majorver = 1 ]; then
        gridftp_option="-a gsi"
      fi
      gridftp_rm_cmdline="${gridftp_rm_command} ${__token_hostname} $gridftp_option \"quote dele ${__token_fullpath}\""
    fi
  fi

  if [ ! -n "${gridftp_rm_cmdline}" ]; then
    log_and_exit 1 "Cannot find gridftp remove application"
  fi

  eval $gridftp_rm_cmdline
  if [ $? -ne 0 ]; then
    log_and_exit 1 "Cannot take token"
  fi

fi

notifyToCream "JobStatus=11;"

notifyToLB "ReallyRunning"

if [ "$PBS_NODEFILE" ]; then
   HOSTFILE=${PBS_NODEFILE}
fi
 
if [ "$LSB_HOSTS" ]; then
   HOSTFILE=host$$
   for hostItem in ${LSB_HOSTS}; do
       echo $hostItem >> ${HOSTFILE}
   done
fi
 
if [ "$HOSTFILE" ]; then
    for hostItem in `cat $HOSTFILE`; do
        ssh $hostItem mkdir -p `pwd`
        /usr/bin/scp -rp ./* $hostItem:`pwd`
        ssh $hostItem chmod 755 /bin/echo
    done
else
    log_and_exit 1 "Cannot find hostfile"
fi

perl -e "${perl_notifier}"'

sub get_exit_value {
  my ($result) = @_;

  my ($exit_value,$signal_num,$dumped_core)  = ( $result >> 8, $result & 0x7F, ($result & 0x80) ? 1 : 0 );
  return $exit_value;
}

sub max {
    if ($_[0]<$_[1]) {return $_[1]} else {return $_[0]};
} 

if (!defined($jobpid = fork())) {

    die "cannot fork job: $!";

} elsif ($jobpid == 0) {

    if (defined($ENV{"EDG_WL_NOSETPGRP"})) {
      $SIG{"TTIN"} = "IGNORE";
      $SIG{"TTOU"} = "IGNORE";
      setpgrp(0, 0);
    }

    exec(@ARGV);
    warn "could not exec $ARGV[0]: $!\n";
    exit(127);

} elsif (defined($chkpid = fork()) && $chkpid == 0) {

    $dtime_slot = $ENV{__delegationTimeSlot};
    if ($dtime_slot != -1) {
      while (1) {
        $grid_proxy_info_output = `grid-proxy-info 2>/dev/null` || 0;
        if ($grid_proxy_info_output =~ /timeleft\s+:\s+(\d+):(\d+):(\d+)/) {
           ($hr,$min,$sec) = ($1,$2,$3);
           $time_left = $hr*3600 + $min*60 + $sec;
        } else {
        $time_left = 0;
        }
        last if ($time_left <= 0);
        $succeed = 1;
        if ($time_left <= $dtime_slot) {
             $succeed = get_exit_value(system("globus-url-copy". " -dcpriv " . $ENV{__delegationProxyCertSandboxPath}. " " . "file://". $ENV{__delegationProxyCertSandboxPathTmp}." >/dev/null 2>&1"));
             if ($succeed == 0) {
                $succeed = get_exit_value(system("chmod 600". " " . $ENV{__delegationProxyCertSandboxPathTmp} ." >/dev/null 2>&1"));
             }
             if ($succeed == 0) {
                $succeed = get_exit_value(system("mv". " " . $ENV{__delegationProxyCertSandboxPathTmp}. " " . $ENV{"X509_USER_PROXY"} . " >/dev/null 2>&1"));
             }
             sleep(max($ENV{__copy_proxy_min_retry_wait}, int((0.2 + 0.1*rand()) * $time_left)));
        } else {
           $time_left = $time_left - ($dtime_slot * (1 + 0.2*rand()));
           sleep($time_left);
        }
      }
    } else {
      $grid_proxy_info_output = `grid-proxy-info 2>/dev/null` || 0;
      if ($grid_proxy_info_output =~ /timeleft\s+:\s+(\d+):(\d+):(\d+)/) {
         ($hr,$min,$sec) = ($1,$2,$3);
         $time_left = $hr*3600 + $min*60 + $sec;
      } else {
        $time_left = 0;
      }
      sleep($time_left);
    }

    if ( length("'${__logger_dest}'")>0 ) {
        send_notify("JobStatus=4;Reason=\"Proxy is expired\";ExitCode=2;");
    }
    print STDERR "Proxy expired: job killed\n";
    if (defined($ENV{"EDG_WL_NOSETPGRP"})){
        kill(9, $jobpid);
    }else{
        kill(-15, getpgrp(0));
    }
    exit(0);

} else {

    waitpid($jobpid, 0);
    $jobstatus=$?/256;
    if (defined($chkpid)) {
        kill(9, $chkpid);
    }
    print "job exit status = ${jobstatus}\n";
    exit($jobstatus);

}' "mpirun -np ${__nodes} -machinefile ${HOSTFILE} ${__cmd_line}"

exit_code=$?

if [ -n "${GLITE_LOCAL_CUSTOMIZATION_DIR}" -a -f "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_2.sh" ]; then
  . "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_2.sh"
fi

if [ -n "${__perusal_filesdesturi}" ]; then
  kill -USR2 $perusal_send_pid
  wait $perusal_send_pid 
fi

if [ -n "${__epilogue}" ]; then
  if [ ! -f "${__epilogue}" ]; then
    log_and_exit 1 "Epilogue script does not exist"
  fi
  chmod +x "${__epilogue}" 2>/dev/null
  ${__epilogue} "${__epilogue_arguments}"
  epilogue_status=$?
  if [ ${epilogue_status} -ne 0 ]; then
    log_and_exit 1 "Epilogue failed with error ${epilogue_status}"
  fi
fi

###################################################################
#                             OSB
###################################################################

#
#  Support for wildcards
#
declare -a __all_output_file
declare -a __output_cmd_line
if [ -n "${__output_file[0]}" ]; then

  file_counter=0
  item_counter=0

  for output_item in "${__output_file[@]}"; do
    missing_item=1
    for output_file in `eval ls -d ${output_item} 2>/dev/null`; do

      if [ -f ${output_file} ]; then

        missing_item=0
        __all_output_file[$file_counter]="$output_file"

        if [ -n "${__gsiftp_dest_uri}" ]; then
          __output_cmd_line[$file_counter]="${globus_transfer_cmd} file://${output_file} ${__gsiftp_dest_uri}`basename $output_file`"
        else
          if [ -n "${__https_dest_uri}" ]; then
            __output_cmd_line[$file_counter]="${https_transfer_cmd} file://${output_file} ${__https_dest_uri}/`basename $output_file`"
          else
            __output_cmd_line[$file_counter]="${__output_transfer_cmd[${item_counter}]} file://${output_file} ${__output_file_dest[${item_counter}]}"
          fi
        fi

        file_counter=$[${file_counter}+1]
      fi
    done
    if [ $missing_item -eq 1 ]; then
      echo "Warning: cannot find ${output_item}"
    fi
    item_counter=$[${item_counter}+1]
  done
fi

#
# end of support for wildcards
#

error_message=""

if [ ${__max_osb_size} -ge 0 ]; then

  file_size_acc=0

  for ((idx=0; idx<${#__all_output_file[*]}; idx++)); do
    file_size=`stat -t ${__all_output_file[$idx]} | awk '{print $2}'`
    file_size_acc=$[$file_size_acc+$file_size]
  done

  if [ ${__max_osb_size} -le $file_size_acc ]; then

    size_diff=$[$file_size_acc-${__max_osb_size}]
    percent=$[$size_diff*100/$file_size_acc]
    percent=$[100-$percent]

    for ((idx=0; idx<${#__all_output_file[*]}; idx++)); do

      file_size=`stat -t ${__all_output_file[$idx]} | awk '{print $2}'`
      tail_size=$[$file_size*$percent/100]
      tail ${__all_output_file[$idx]} --bytes=$tail_size>${__all_output_file[$idx]}.tail 2>/dev/null
      if [ $? != 0 ]; then
        echo "Cannot truncate file ${__all_output_file[$idx]}"
        rm ${__all_output_file[$idx]}
        touch ${__all_output_file[$idx]}
      else
        mv ${__all_output_file[$idx]}.tail ${__all_output_file[$idx]}
      fi

    done

  fi
  
fi

for((idx=0; idx<${#__output_cmd_line[*]}; idx++)); do
  retry_copy ${__output_cmd_line[$idx]}
  result=$?
  if [ $result != 0 ]; then
    error_message="Cannot move OSB (${__output_cmd_line[$idx]}): ${retry_error_message}"
    echo ${error_message}
  fi
done

if [ -n "${GLITE_LOCAL_CUSTOMIZATION_DIR}" -a -f "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_3.sh" ]; then
  . "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_3.sh"
fi

if [ -n "$error_message" ]; then
  log_and_exit 1 "$error_message" $exit_code
fi

notifyToCream "JobStatus=4;Reason=\"reason=0\";ExitCode=${exit_code};"

if [ -n "$GLITE_WMS_SEQUENCE_CODE" -a -f "$LB_LOGEVENT" ]; then
  GLITE_WMS_SEQUENCE_CODE=`$LB_LOGEVENT \
                           --jobid="$GLITE_WMS_JOBID" \
                           --source=LRMS \
                           --sequence="$GLITE_WMS_SEQUENCE_CODE" \
                           --event="Done" \
                           --reason="job completed" \
                           --status_code=OK \
                           --exit_code=0 \
                           || echo $GLITE_WMS_SEQUENCE_CODE`
fi

do_exit 0

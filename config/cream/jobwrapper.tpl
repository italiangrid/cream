###################################################################
#                            Signals
###################################################################
 
trap 'fatal_error "Job has been terminated (got SIGXCPU)" "OSB"' XCPU
trap 'fatal_error "Job has been terminated (got SIGQUIT)" "OSB"' QUIT
trap 'fatal_error "Job has been terminated (got SIGINT)" "OSB"' INT
trap 'fatal_error "Job has been terminated (got SIGTERM)" "OSB"' TERM
###################################################################
#                           Environment
###################################################################

#trick. 
export EDG_WL_NOSETPGRP=1 

export GLITE_WMS_LOCATION=${GLITE_LOCATION:-/opt/glite}
export GLITE_WMS_JOBID=${__gridjobid}
export GLITE_WMS_SEQUENCE_CODE="$1"
export GLITE_WMS_LOG_DESTINATION=${__ce_hostname}
shift

jw_host="`hostname -f`"

for((idx=0; idx<${#__environment[*]}; idx++)); do
  eval export ${__environment[$idx]}
done

#if [ -z "${GLITE_LOCAL_MAX_OSB_SIZE}" ]; then
#  __max_osb_size=-1
#else
#  __max_osb_size=${GLITE_LOCAL_MAX_OSB_SIZE}
#fi

if [ -z "${GLITE_LOCAL_COPY_RETRY_COUNT_ISB}" ]; then
  if [ -n "${GLITE_LOCAL_COPY_RETRY_COUNT}" ]; then
    __copy_retry_count_isb=${GLITE_LOCAL_COPY_RETRY_COUNT}
  fi
else
  __copy_retry_count_isb=${GLITE_LOCAL_COPY_RETRY_COUNT_ISB}
fi

if [ -z "${GLITE_LOCAL_COPY_RETRY_COUNT_OSB}" ]; then
  if [ -n "${GLITE_LOCAL_COPY_RETRY_COUNT}" ]; then
    __copy_retry_count_osb=${GLITE_LOCAL_COPY_RETRY_COUNT}
  fi
else
  __copy_retry_count_osb=${GLITE_LOCAL_COPY_RETRY_COUNT_OSB}
fi
  
if [ -z "${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT_ISB}" ]; then
  if [ -n "${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT}" ]; then
    __copy_retry_first_wait_isb=${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT}
  fi
else
  __copy_retry_first_wait_isb=${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT_ISB}
fi

if [ -z "${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT_OSB}" ]; then
  if [ -n "${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT}" ]; then
    __copy_retry_first_wait_osb=${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT}
  fi
else
  __copy_retry_first_wait_osb=${GLITE_LOCAL_COPY_RETRY_FIRST_WAIT_OSB}
fi

USR2_signal_received=0;

###################################################################
#                           functions definitions
###################################################################

do_transfer() # 1 - command, 2 - source, 3 - dest, 4 - std err, 5 - exit code file
{
  eval "$1" "$2" "$3" 2>"$4"
  echo $? > "$5"
}
###################################################################

doExit() # 1 - status, 2 - reason 
{
  jw_status=$1
  echo "jw exit status = ${jw_status}"
  echo $2 1>&2

  if [ $__create_subdir -eq 1 ]; then
    cd ..
    rm -rf ${newdir}
  fi

  # customization point #3
  if [ -n "${GLITE_LOCAL_CUSTOMIZATION_DIR}" ]; then
    if [ -r "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_3.sh" ]; then
      . "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_3.sh"
    fi
  fi
  
  exit ${jw_status}
}
###################################################################

retry_copy() # 1 - command, 2 - source, 3 - dest
{
  count=0
  succeded=1
  sleep_time=0
  source="$2"
  dest="$3"

  local match_index=`expr match "${source}" '[[:alpha:]][[:alnum:]+.-]*://'`
  if [ ${match_index} -gt 0 ]; then
    match_index=`expr ${match_index} - 3`
  fi
  local scheme_src=${source:0:${match_index}}

  local match_index=`expr match "${dest}" '[[:alpha:]][[:alnum:]+.-]*://'`
  if [ ${match_index} -gt 0 ]; then
    match_index=`expr ${match_index} - 3`
  fi
  local scheme_dest=${dest:0:${match_index}}
  
   if [ "x${scheme_src}" == "xfile" -o "x${scheme_src}" == "x" ]; then
    __copy_retry_count=${__copy_retry_count_osb}
    __copy_retry_first_wait=${__copy_retry_first_wait_osb}
  elif [ "x${scheme_dest}" == "xfile" -o "x${scheme_dest}" == "x" ]; then
    __copy_retry_count=${__copy_retry_count_isb}
    __copy_retry_first_wait=${__copy_retry_first_wait_isb}
  fi    
  
  while [ $count -le ${__copy_retry_count} -a $succeded -ne 0 ];
  do
    time_left=`grid-proxy-info -timeleft 2>/dev/null || echo -100`;
    # -100 if there's some problem to detect the lifetime of the proxy.

    if [ $time_left -eq -100 ]; then
      retry_error_message="Problem to detect the lifetime of the proxy"
      return 1
    fi

    if [ $time_left -lt $sleep_time ]; then
      retry_error_message="proxy expired"
      return 1
    fi
    
    sleep $sleep_time & sleep_time_pid=$!
    trap 'USR2_signal_received=1; kill -ALRM $sleep_time_pid >/dev/null 2>&1' USR2
    wait $sleep_time_pid >/dev/null 2>&1

    if [ $USR2_signal_received -ne 0 ]; then
       succeded=1;
       break;
    fi
    
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
    do_transfer "$1" "${source}" "${dest}" "$transfer_stderr" "$transfer_exitcode"&
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
      else
        retry_error_message=`cat $transfer_stderr 2>/dev/null`
      fi
    fi
    rm -f "$transfer_stderr" "$transfer_exitcode"
    count=`expr $count + 1`
  done
  return ${succeded}
}
###################################################################

doDSUploadTmp()
{
  local filename="${__dsupload}"
  echo "#" >> "$filename.tmp"
  echo "# Autogenerated by JobWrapper!" >> "$filename.tmp"
  echo "#" >> "$filename.tmp"
  echo "# The file contains the results of the upload and registration" >> "$filename.tmp"
  echo "# process in the following format:" >> "$filename.tmp"
  echo "# <outputfile> <lfn|guid|Error>" >> "$filename.tmp"
  echo "" >> "$filename.tmp"
}
###################################################################

doDSUpload()
{
  local filename="${__dsupload}"
  mv -fv "$filename.tmp" "$filename"
}
###################################################################

doCheckReplicaFile() # 1 - OD_outputfile
{
  local sourcefile=$1
  local filename="${__dsupload}"
  local exit_status=0
  if [ ! -f "${workdir}/$sourcefile" ]; then
    echo "$sourcefile    Error: File $sourcefile has not been found on the WN $jw_host" >> "$filename.tmp"
    echo >> "$filename.tmp"
    exit_status=1
  fi
  return $exit_status
}
###################################################################

doReplicaFile() # 1 - OD_outputfile
{
  local sourcefile=$1
  local filename="${__dsupload}"
  local exit_status=0
  localf=`${lcg_rm_command} --vo=${__vo} copyAndRegisterFile "file://${workdir}/$sourcefile" 2>&1`
  result=$?
  if [ $result -eq 0 ]; then
    echo "$sourcefile    $localf" >> "$filename.tmp"
  else
    echo "$sourcefile    Error: $localf" >> "$filename.tmp"
    exit_status=1
  fi
  echo >> "$filename.tmp"
  return $exit_status
}
###################################################################

doReplicaFilewithLFN() # 1 - OD_outputfile, 2 - LFN
{
  local sourcefile="$1"
  local lfn="$2"
  local filename="${__dsupload}"
  local exit_status=0
  localf=`${lcg_rm_command} --vo=${__vo} copyAndRegisterFile "file://${workdir}/$sourcefile" -l "$lfn" 2>&1`
  result=$?
  if [ $result -eq 0 ]; then
    echo "$sourcefile    $lfn" >> "$filename.tmp"
  else
    localnew=`${lcg_rm_command} --vo=${__vo} copyAndRegisterFile "file://${workdir}/$sourcefile" 2>&1`
    result=$?
    if [ $result -eq 0 ]; then
      echo "$sourcefile    $localnew" >> "$filename.tmp"
    else
      echo "$sourcefile    Error: $localf; $localnew" >> "$filename.tmp"
      exit_status=1
    fi
  fi
  echo >> "$filename.tmp"
  return $exit_status
}
###################################################################

doReplicaFilewithSE() # 1 - OD_outputfile, 2 - SE
{
  local sourcefile="$1"
  local se="$2"
  local filename="${__dsupload}"
  local exit_status=0
  localf=`${lcg_rm_command} --vo=${__vo} copyAndRegisterFile "file://${workdir}/$sourcefile" -d "$se" 2>&1`
  result=$?
  if [ $result -eq 0 ]; then
    echo "$sourcefile    $localf" >> "$filename.tmp"
  else
    localnew=`${lcg_rm_command} --vo=${__vo} copyAndRegisterFile "file://${workdir}/$sourcefile" 2>&1`
    result=$?
    if [ $result -eq 0 ]; then
      echo "$sourcefile    $localnew" >> "$filename.tmp"
    else
      echo "$sourcefile    Error: $localf; $localnew" >> "$filename.tmp"
      exit_status=1
    fi
  fi
  echo >> "$filename.tmp"
  return $exit_status
}
###################################################################

doReplicaFilewithLFNAndSE() # 1 - OD_outputfile, 2 - LFN, 3 - SE
{
  local sourcefile="$1"
  local lfn="$2"
  local se="$3"
  local filename="${__dsupload}"
  local exit_status=0
  localf=`${lcg_rm_command} --vo=${__vo} copyAndRegisterFile "file://${workdir}/$sourcefile" -l "$lfn" -d "$se" 2>&1`
  result=$?
  if [ $result -eq 0 ]; then
    echo "$sourcefile    $localf" >> "$filename.tmp"
  else
    localse=`${lcg_rm_command} --vo=${__vo} copyAndRegisterFile "file://${workdir}/$sourcefile" -d "$se" 2>&1`
    result=$?
    if [ $result -eq 0 ]; then
      echo "$sourcefile    $localse" >> "$filename.tmp"
    else
      locallfn=`${lcg_rm_command} --vo=${__vo} copyAndRegisterFile "file://${workdir}/$sourcefile" -l "$lfn" 2>&1`
      result=$?
      if [ $result -eq 0 ]; then 
        echo "$sourcefile    $locallfn" >> "$filename.tmp"
      else
        localnew=`${lcg_rm_command} --vo=${__vo} copyAndRegisterFile "file://${workdir}/$sourcefile" 2>&1`
        result=$?
        if [ $result -eq 0 ]; then
          echo "$sourcefile    $localnew" >> "$filename.tmp"
        else
          echo "$sourcefile    Error: $localf; $localse; $locallfn; $localnew" >> "$filename.tmp"
          exit_status=1
        fi    
      fi
    fi
  fi
  echo >> "$filename.tmp"
  return $exit_status
}
###################################################################

function send_partial_file {
  local TRIGGERFILE=$1
  local DESTURL=$2
  local POLL_INTERVAL=$3
  local FILESUFFIX=partialtrf
  local GLOBUS_RETURN_CODE
  local SLICENAME
  local LISTFILE=`pwd`/listfile.$$
  local SLEEP_PID
  local MD5
  local OLDSIZE
  local NEWSIZE
  local COUNTER

  while [ $USR2_signal_received -eq 0 ] ; do

    sleep $POLL_INTERVAL>/dev/null 2>&1 & SLEEP_PID=$!
    trap 'USR2_signal_received=1; kill -ALRM $SLEEP_PID >/dev/null 2>&1' USR2
    wait $SLEEP_PID >/dev/null 2>&1
  
    if [ $USR2_signal_received -eq 1 ]; then
      break;
    fi

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
###################################################################

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
###################################################################

function notifyToCream {
  if [ -n "${__logger_dest}" ]; then
    perl -e "${perl_notifier}"' send_notify(@ARGV);' "$1"
    return $?
  fi
  return 0
}
###################################################################

jw_echo() # 1 - msg
{
  echo "$1"
}
###################################################################

log_event() # 1 - event 
{
  if [ -n "$GLITE_WMS_SEQUENCE_CODE" -a -f "$LB_LOGEVENT" ]; then
    export GLITE_WMS_SEQUENCE_CODE=`$LB_LOGEVENT \
                               --jobid="$GLITE_WMS_JOBID" \
                               --source=LRMS \
                               --sequence="$GLITE_WMS_SEQUENCE_CODE" \
                               --event="$1" \
                               --node="$jw_host" \
                             || echo $GLITE_WMS_SEQUENCE_CODE`
  fi
}
###################################################################

log_user_event() # 1 - reason
{
   if [ -n "$GLITE_WMS_SEQUENCE_CODE" -a -f "$LB_LOGEVENT" ]; then
     export GLITE_WMS_SEQUENCE_CODE=`$LB_LOGEVENT \
                                --jobid="$GLITE_WMS_JOBID"\
                                --source=LRMS\
                                --sequence="$GLITE_WMS_SEQUENCE_CODE"\
                                --event="UserTag"\
                                --name="notice"\
                                --value="$1"\
                                --node="$jw_host"\
                              || echo $GLITE_WMS_SEQUENCE_CODE`
   fi
}
###################################################################

log_done_failed() # 1 - exit code, 2 reason
{
 if [ -n "$GLITE_WMS_SEQUENCE_CODE" -a -f "$LB_LOGEVENT" ]; then
  export GLITE_WMS_SEQUENCE_CODE=`$LB_LOGEVENT\
                             --jobid="$GLITE_WMS_JOBID"\
                             --source=LRMS\
                             --sequence="$GLITE_WMS_SEQUENCE_CODE"\
                             --event="Done"\
                             --reason="$2"\
                             --status_code=FAILED\
                             --exit_code="$1"\
                           || echo $GLITE_WMS_SEQUENCE_CODE`
  jw_echo "LM_log_done_begin $2 LM_log_done_end"
 fi
}
###################################################################

log_done_ok() # 1 - exit code, 2 reason
{
 if [ -n "$GLITE_WMS_SEQUENCE_CODE" -a -f "$LB_LOGEVENT" ]; then
  export GLITE_WMS_SEQUENCE_CODE=`$LB_LOGEVENT\
                             --jobid="$GLITE_WMS_JOBID"\
                             --source=LRMS\
                             --sequence="$GLITE_WMS_SEQUENCE_CODE"\
                             --event="Done"\
                             --reason="$2"\
                             --status_code=OK\
                             --exit_code="$1"\
                           || echo $GLITE_WMS_SEQUENCE_CODE`
  jw_echo "LM_log_done_begin $2 LM_log_done_end"
 fi
}
###################################################################

fatal_error() # 1 - reason, 2 - transfer OSB
{
  notifyToCream "JobStatus=4; Reason=\"$1\";"
  log_done_failed 1 "$1"
  if [ "x$2" == "xOSB" ]; then
    OSB_transfer
  fi
  doExit 1 "$1"
}
###################################################################

truncate() # 1 - file name, 2 - bytes num., 3 - name of the truncated file
{
  tail "$1" --bytes=$2>$3 2>/dev/null
  return $?
}
###################################################################

OSB_transfer()
{
# Begin: Support for wildcards
declare -a __all_output_file
declare -a __output_cmd_line_command
declare -a __output_cmd_line_source
declare -a __output_cmd_line_dest

if [ -n "${__output_file[0]}" ]; then

  warning_message=""
  file_counter=0
  item_counter=0

  for output_item in "${__output_file[@]}"; do
    missing_item=1
    for output_file in `eval ls -d ${output_item} 2>/dev/null`; do

      if [ -f ${output_file} ]; then

        missing_item=0
        __all_output_file[$file_counter]="$output_file"

        if [ -n "${__gsiftp_dest_uri}" ]; then
          __output_cmd_line_command[$file_counter]="${__output_transfer_cmd[${item_counter}]}"
          __output_cmd_line_source[$file_counter]="file://${output_file}"
          __output_cmd_line_dest[$file_counter]="${__gsiftp_dest_uri}`basename $output_file`"
        else
          if [ -n "${__https_dest_uri}" ]; then
            __output_cmd_line_command[$file_counter]="${https_transfer_cmd}"
            __output_cmd_line_source[$file_counter]="file://${output_file}"
            __output_cmd_line_dest[$file_counter]="${__https_dest_uri}/`basename $output_file`"
          else
            __output_cmd_line_command[$file_counter]="${__output_transfer_cmd[${item_counter}]}"
            __output_cmd_line_source[$file_counter]="file://${output_file}"
            __output_cmd_line_dest[$file_counter]="${__output_file_dest[${item_counter}]}"
          fi
        fi

        file_counter=$[${file_counter}+1]
      fi
    done
    if [ $missing_item -eq 1 ]; then
      warning_message="Cannot read or missing file ${output_item}"
      log_user_event "$warning_message"
      echo "$warning_message"
    fi
    item_counter=$[${item_counter}+1]
  done
fi

# End: Support for wildcards

  error_message=""
  file_size_acc=0
  
  for ((idx=0; idx<${#__all_output_file[*]}; idx++)); do
    local match_index=`expr match "${__output_cmd_line_dest[idx]}" '[[:alpha:]][[:alnum:]+.-]*://'`
    local scheme_dest=${__output_cmd_line_dest[idx]:0:${match_index}}
    local remaining_dest=${__output_cmd_line_dest[idx]:${#scheme_dest}:${#__output_cmd_line_dest[idx]}-${#scheme_dest}}
    local hostname=${remaining_dest:0:`expr match "$remaining_dest" '[[:alnum:]_.~!$&()-]*'`}

    if [[ ${__max_osb_size} -ge 0 ]] && [[ $hostname == ${__wms_hostname} ]]; then
    
       file_size=`stat -t ${__all_output_file[$idx]} | awk '{print $2}'`
       if [ -z "$file_size" ]; then
          file_size=0
       fi
       file_size_acc=`expr $file_size_acc + $file_size`
       
       if [[ $file_size_acc -le ${__max_osb_size} ]]; then
          retry_copy "${__output_cmd_line_command[$idx]}" "${__output_cmd_line_source[$idx]}" "${__output_cmd_line_dest[$idx]}"
       else
          error_message="OSB quota exceeded for ${__all_output_file[$idx]}, truncating needed"
          log_user_event "$error_message"
          echo "$error_message"
          file_size_acc=`expr $file_size_acc - $file_size`
          #idx=current_file
          remaining_files=`expr ${#__all_output_file[*]} \- $idx`
          remaining_space=`expr ${__max_osb_size} \- $file_size_acc`
          trunc_len=`expr $remaining_space / $remaining_files`
          if [ $? != 0 ]; then
            trunc_len=0
          fi
          file_size_acc=`expr $file_size_acc + $trunc_len`
          truncate "${__all_output_file[$idx]}" $trunc_len "${__all_output_file[$idx]}.tail"
          
          if [ $? != 0 ]; then
            error_message="Could not truncate output sandbox file ${__all_output_file[$idx]}, not sending"
            log_user_event "$error_message" 
            echo "$error_message" 
          else
            error_message="Truncated last $trunc_len bytes for file ${__all_output_file[$idx]}"
            mv ${__all_output_file[$idx]}.tail ${__all_output_file[$idx]}
            log_user_event "$error_message" 
            echo "$error_message" 
            retry_copy "${__output_cmd_line_command[$idx]}" "${__output_cmd_line_source[$idx]}" "${__output_cmd_line_dest[$idx]}.tail"
          fi
       fi   
     else # unlimited osb
        retry_copy "${__output_cmd_line_command[$idx]}" "${__output_cmd_line_source[$idx]}" "${__output_cmd_line_dest[$idx]}"
     fi
     if [ $? != 0 ]; then
        fatal_error "Cannot upload ${__output_cmd_line_source[$idx]} into ${__output_cmd_line_dest[$idx]}"
     fi
  done
  
  #LRMS OSB
  for((idx=0; idx<${#__lrms_output_file[*]}; idx++)); do
    `mv ${workdir}/${__lrms_output_file[$idx]} ${workdir}/../` 
    if [ $? != 0 ]; then
      #fatal_error "Cannot move ${workdir}/${__lrms_output_file[$idx]} to ${workdir}/../"
      warning_message="Cannot move ${workdir}/${__lrms_output_file[$idx]} to ${workdir}/../"
      log_user_event "$warning_message"
      echo "$warning_message"
    fi
  done
}

###################################################################
#                           let's start it up
###################################################################

for lb_logevent_command in "${GLITE_WMS_LOCATION}/bin/glite-lb-logevent" \
                           "/usr/bin/glite-lb-logevent" \
                           "`which glite-lb-logevent 2>/dev/null`"; do
  if [ -x "${lb_logevent_command}" ]; then
     break;
  fi
done

LB_LOGEVENT=${lb_logevent_command}

# customization point #1
# Be sure to update workdir as it may be changed by cp_1.sh
if [ -n "${GLITE_LOCAL_CUSTOMIZATION_DIR}" ]; then
  if [ -r "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_1.sh" ]; then
    . "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_1.sh"
  fi
fi

notifyToCream "JobStatus=2;"
log_event "Running"

#################### BEGIN VO-HOOK #################################
vo_hook="lcg-jobwrapper-hook.sh" # common-agreed now hard-coded

if [ -z "${__ce_application_dir}" -a -n "${__vo}" ]; then
     tmp=`echo "${__vo}" | sed 's/[-.]/_/g' | tr '[a-z]' '[A-Z]'`
     eval __ce_application_dir=\$VO_${tmp}_SW_DIR
fi
if [ -n "${__ce_application_dir}" ]; then
  if [ -d "${__ce_application_dir}" ]; then
    if [ -r "${__ce_application_dir}/${vo_hook}" ]; then
      . "${__ce_application_dir}/${vo_hook}"
    elif [ -r "${__ce_application_dir}/${__vo}/${vo_hook}" ]; then
      . "${__ce_application_dir}/${__vo}/${vo_hook}"
    else
      jw_echo "${vo_hook} not readable or not present"
    fi
  else
    jw_echo "${__ce_application_dir} not found or not a directory"
  fi
fi
unset vo_hook
#################### END VO-HOOK ###################################

if [ $__create_subdir -eq 1 ]; then
  newdir="${__working_directory}"
  mkdir ${newdir}
  cd ${newdir}
fi

touch .tmp_file
if [ $? -ne 0 ]; then
  fatal_error "Working directory not writable"
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
  fatal_error "Cannot find grid-proxy-info"
fi

###################################################################
#                             ISB
###################################################################

for((idx=0; idx<${#__input_transfer_cmd[*]}; idx++)); do
  #input_cmd_line="${__input_transfer_cmd[$idx]} ${__input_file_url[$idx]} file://${workdir}/${__input_file_dest[$idx]}"
  retry_copy "${__input_transfer_cmd[$idx]}" "${__input_file_url[$idx]}" "file://${workdir}/${__input_file_dest[$idx]}"
  if [ $? != 0 ]; then
    fatal_error "Cannot move ISB (retry_copy ${__input_transfer_cmd[$idx]} ${__input_file_url[$idx]} file://${workdir}/${__input_file_dest[$idx]}): ${retry_error_message}"
  fi
done

#LRMS ISB
for((idx=0; idx<${#__lrms_input_file[*]}; idx++)); do
  `mv ${workdir}/../${__lrms_input_file[$idx]} ${workdir}` 
  if [ $? != 0 ]; then
    fatal_error "Cannot move ${workdir}/../${__lrms_input_file[$idx]} to ${workdir}"
  fi
done

###################################################################
#                             JOB
###################################################################

# Setting of GLITE_WMS_RB_BROKERINFO 
if [ -f "`pwd`/${__brokerinfo}" ]; then
  export GLITE_WMS_RB_BROKERINFO="`pwd`/${__brokerinfo}"
fi


if [ -e "${__executable}" ]; then
   chmod +x "${__executable}" 2> /dev/null
else
   fatal_error "${__executable} not found or unreadable"
fi

for item in "glite-wms-pipe-input" "glite-wms-pipe-output" "glite-wms-job-agent"; do
  if [ -f "$item" ]; then
    chmod u+x $item;
  fi
done

if [ -n "${__prologue}" ]; then
  if [ ! -f "${__prologue}" ]; then
    fatal_error "Prologue script does not exist"
  fi
  chmod +x "${__prologue}" 2>/dev/null
  ${__prologue} ${__prologue_arguments}
  prologue_status=$?
  if [ ${prologue_status} -ne 0 ]; then
    fatal_error "Prologue failed with error ${prologue_status}"
  fi
fi

if [ -n "${__perusal_filesdesturi}" ]; then
  send_partial_file ${__perusal_listfileuri} ${__perusal_filesdesturi} ${__perusal_timeinterval} & 
  perusal_send_pid=$!
fi

if [ -n "${__token_file}" ]; then

  gridftp_rm_cmdline=""
  
  gridftp_rm_command=`which uberftp 2>/dev/null`
  if [ -x "$gridftp_rm_command" ]; then
     majorver=`$gridftp_rm_command -version | perl -nle 'print $1 if /(\d+)+(\.)*/'`
     gridftp_option=
     if [ $majorver = 1 ]; then
       gridftp_option="-a gsi"
     fi
     gridftp_rm_cmdline="${gridftp_rm_command} ${__token_hostname} $gridftp_option \"quote dele ${__token_fullpath}\""
  fi

  if [ ! -n "${gridftp_rm_cmdline}" ]; then    
    for gridftp_rm_command in ${GLITE_LOCATION:-/opt/glite}/bin/glite-gridftp-rm \
                              `which glite-gridftp-rm 2>/dev/null` \
                              /usr/bin/glite-gridftp-rm ; do
       if [ -x "$gridftp_rm_command" ]; then
          gridftp_rm_cmdline="${gridftp_rm_command} ${__token_file}"
          break;
       fi
    done
  fi

  if [ ! -n "${gridftp_rm_cmdline}" ]; then
    fatal_error "Cannot find gridftp remove application"
  fi

  count=0
  succeded=1
  sleep_time=0
  while [ $count -lt 3 -a $succeded -ne 0 ];
  do
    sleep $sleep_time
    eval $gridftp_rm_cmdline
    if [ $? -ne 0 ]; then
      sleep_time=60
      count=`expr $count + 1`
    else
      succeded=0
    fi
  done
  if [ $succeded -ne 0 ]; then
    fatal_error "Cannot take token"
  fi
 
fi

notifyToCream "JobStatus=11;"

log_event "ReallyRunning"

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
    $time_left_error = 0;
    $time_left = 0; 

    if ($dtime_slot != -1) {
        while (1) {
            $grid_proxy_info_output = `grid-proxy-info 2>/dev/null` || -100;
            if ($grid_proxy_info_output == -100) {
                $time_left_error = -1;
                $time_left = -1;
            }

            last if ($time_left == -1);

            if ($grid_proxy_info_output =~ /timeleft\s+:\s+(\d+):(\d+):(\d+)/) {
               ($hr,$min,$sec) = ($1,$2,$3);
               $time_left = $hr*3600 + $min*60 + $sec;
               #$currenttime=`date`;
            } else {
                $time_left_error = -1;
                $time_left = 0;
            }

            last if ($time_left <= 0);

            $succeed = 1;
            if ($time_left <= $dtime_slot) {
                $currenttime = localtime time;
                print "$currenttime: Downloading proxy from CE node ...\n";
                $succeed = get_exit_value(system("globus-url-copy". " -dcpriv " . $ENV{__delegationProxyCertSandboxPath}. " " . "file://". $ENV{__delegationProxyCertSandboxPathTmp}." >/dev/null 2>&1"));

                if ($succeed == 0) {
                    $succeed = get_exit_value(system("chmod 600". " " . $ENV{__delegationProxyCertSandboxPathTmp} ." >/dev/null 2>&1"));
                } else {
                    $currenttime = localtime time;
                    print STDERR "$currenttime: Failure in downloading proxy from CE node\n";
                    print "$currenttime: Failure in downloading proxy from CE node\n";
                }

                if ($succeed == 0) {
                    $succeed = get_exit_value(system("mv". " " . $ENV{__delegationProxyCertSandboxPathTmp}. " " . $ENV{"X509_USER_PROXY"} . " >/dev/null 2>&1"));
                    $currenttime = localtime time;
                    print "$currenttime: Done: proxy downloaded from CE node\n";
                }
                sleep(max($ENV{__copy_proxy_min_retry_wait}, int((0.2 + 0.1*rand()) * $time_left)));
            } else {
                $time_left = $time_left - ($dtime_slot * (1 - 0.2*rand()));
                sleep($time_left);
            }
        }
    } else {
        $grid_proxy_info_output = `grid-proxy-info 2>/dev/null` || 0;

        if ($grid_proxy_info_output =~ /timeleft\s+:\s+(\d+):(\d+):(\d+)/) {
            ($hr,$min,$sec) = ($1,$2,$3);
            $time_left = $hr*3600 + $min*60 + $sec;
            sleep($time_left);
        } else {
            $time_left_error = -1;
            $time_left = 0;
        }
    }
    
    if ( length("'${__logger_dest}'")>0 ) {
        if ($time_left_error == -1){
             send_notify("JobStatus=4;Reason=\"Problem to detect the lifetime of the proxy\";ExitCode=2;");
        } else {
             send_notify("JobStatus=4;Reason=\"Proxy is expired\";ExitCode=2;");
        }
    }

    if ($time_left_error == -1){
        print STDERR "Problem to detect the lifetime of the proxy: job killed\n";
    } else {
         print STDERR "Proxy expired: job killed\n";
    }
    
    if (defined($ENV{"EDG_WL_NOSETPGRP"})) {
        kill(-15, getpgrp(0));
    } else {
        kill(9, $jobpid);
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

}' "${__cmd_line}"

exit_code=$?

if [ -n "${GLITE_LOCAL_CUSTOMIZATION_DIR}" -a -f "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_2.sh" ]; then
  . "${GLITE_LOCAL_CUSTOMIZATION_DIR}/cp_2.sh"
fi

if [ -n "${__perusal_filesdesturi}" ]; then
  kill -USR2 $perusal_send_pid
  wait $perusal_send_pid 
fi

status=$exit_code
#################### BEGIN OUTPUTDATA #################################
if [ ${__output_data} -eq 1 ]; then
   if [ $status -eq 0 ]; then
      doDSUploadTmp
      status=$?
      local_cnt=0
      for lcg_rm_command in "${GLITE_LOCATION}/bin/lcg-replica-manager" \
                          "/usr/bin/lcg-replica-manager" \
                          "`which lcg-replica-manager 2>/dev/null`"; do
        if [ -x "${lcg_rm_command}" ]; then
           break;
        fi
      done
      if [ ! -x "${lcg_rm_command}" ]; then
         fatal_error "Cannot find lcg-replica-manager command"
      else
         for OD_output_file in ${__OD_output_file[@]}
         do
           doCheckReplicaFile ${OD_output_file}
           status=$?
           if [ $status -eq 0 ]; then
              if [ -z "${__OD_logical_filename[$local_cnt]}" -a -z "${__OD_storage_element[$local_cnt]}" ] ; then
                 local=`doReplicaFile ${OD_output_file}`
              elif [ -n "${__OD_logical_filename[$local_cnt]}" -a -z "${__OD_storage_element[$local_cnt]}" ] ; then
                 local=`doReplicaFilewithLFN ${OD_output_file} ${__OD_logical_filename[$local_cnt]}`
              elif [ -z "${__OD_logical_filename[$local_cnt]}" -a -n "${__OD_storage_element[$local_cnt]}" ] ; then
                 local=`doReplicaFilewithSE ${OD_output_file} ${__OD_storage_element[$local_cnt]}`
              else
                 local=`doReplicaFilewithLFNAndSE ${OD_output_file} ${__OD_logical_filename[$local_cnt]} ${__OD_storage_element[$local_cnt]}`
              fi
              status=$?
           fi
           let "++local_cnt"
         done
         doDSUpload
         status=$?
      fi
   fi
fi
#################### END OUTPUTDATA ###################################

if [ -n "${__epilogue}" ]; then
  if [ ! -r "${__epilogue}" ]; then
    fatal_error "Epilogue ${__epilogue} not found"
  fi
  chmod +x "${__epilogue}" 2>/dev/null
  ${__epilogue} ${__epilogue_arguments}
  epilogue_status=$?
  if [ ${epilogue_status} -ne 0 ]; then
    fatal_error "Epilogue failed with error ${epilogue_status}"
  fi
fi

OSB_transfer

notifyToCream "JobStatus=4;Reason=\"reason=0\";ExitCode=${exit_code};"

log_done_ok "${status}" "job completed"
doExit 0

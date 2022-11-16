INPUT_PATH=$(realpath $1)
OUTPUT_PATH=$(realpath $2)

find $INPUT_PATH -name '*.nglog*' -print0 | while read -d $'\0' file
do
    FILE_PATH=$(realpath $file)
    RELATIVE=${FILE_PATH#"$INPUT_PATH"}
    OUTPUT_FILE="$OUTPUT_PATH$RELATIVE"
    OUTPUT_DIR=$(dirname $OUTPUT_FILE)
    echo $OUTPUT_FILE

    if [[ ! -e $OUTPUT_DIR ]]; then
        mkdir -p $OUTPUT_DIR
        touch $OUTPUT_FILE
    fi

   grep 'RETRIEVE\|QUERY' $file > $OUTPUT_FILE
done
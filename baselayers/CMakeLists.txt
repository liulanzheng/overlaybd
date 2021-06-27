add_custom_command(
    OUTPUT ${EXECUTABLE_OUTPUT_PATH}/.commit
    COMMAND tar -zxvf ${CMAKE_CURRENT_LIST_DIR}/ext4_256.tar.gz -C ${EXECUTABLE_OUTPUT_PATH}
    VERBATIM
)

add_custom_target(baselayer ALL DEPENDS ${EXECUTABLE_OUTPUT_PATH}/.commit)

install(FILES ${EXECUTABLE_OUTPUT_PATH}/.commit DESTINATION /opt/overlaybd/baselayers)
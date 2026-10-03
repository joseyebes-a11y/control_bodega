!macro microcellerRepairLegacyUninstaller
  ; These releases used the same app identity and uninstall protocol, but
  ; Linux builds could embed a bad CRC. Run a verified uninstaller from our
  ; temporary directory instead. Do not overwrite the existing installation
  ; before the standard atomic update has succeeded.
  !insertmacro readReg $R6 "$rootKey" "${UNINSTALL_REGISTRY_KEY}" DisplayVersion
  ${If} $R6 == "1.1.0"
  ${OrIf} $R6 == "1.2.0"
  ${OrIf} $R6 == "1.2.1"
  ${OrIf} $R6 == "1.2.2"
    ${If} $installationDir != ""
    ${AndIf} $uninstallerFileName == "$installationDir\${UNINSTALL_FILENAME}"
      File "/oname=$PLUGINSDIR\microceller-repair-uninstaller.exe" "${UNINSTALLER_OUT_FILE}"
      StrCpy $uninstallerFileName "$PLUGINSDIR\microceller-repair-uninstaller.exe"
      DetailPrint "Preparando la actualización con un desinstalador verificado."
    ${Else}
      MessageBox MB_OK|MB_ICONSTOP "La instalación anterior tiene una ruta inesperada. Se conservan sus archivos. Revisa la instalación antes de continuar." /SD IDOK
      SetErrorLevel 2
      Quit
    ${EndIf}
  ${EndIf}
!macroend

!macro customUnInit
  ; This installer only removes the application. Winery data must be managed
  ; explicitly from the application, never by an uninstall command-line flag.
  ${If} ${isDeleteAppData}
    MessageBox MB_OK|MB_ICONSTOP "La desinstalación conserva los datos de la bodega. No se admite borrarlos desde el desinstalador." /SD IDOK
    SetErrorLevel 2
    Quit
  ${EndIf}
!macroend
!macro customCheckAppRunning
  ; Do not terminate Electron or its database worker during a write/backup.
  ${Do}
    nsProcess::_FindProcess "${APP_EXECUTABLE_FILENAME}"
    Pop $R0
    nsProcess::_Unload
    ${If} $R0 == 603
      ${Break}
    ${EndIf}
    ${If} ${Silent}
      SetErrorLevel 2
      Quit
    ${EndIf}
    ${If} $R0 == 0
      MessageBox MB_RETRYCANCEL|MB_ICONEXCLAMATION "Guarda tus cambios y cierra MicroCellerStudio desde Archivo > Salir. Después pulsa Reintentar." /SD IDCANCEL IDRETRY microceller_process_retry
    ${Else}
      MessageBox MB_RETRYCANCEL|MB_ICONSTOP "No se pudo comprobar si MicroCellerStudio está cerrado. Se conservan los archivos. Cierra la aplicación y pulsa Reintentar." /SD IDCANCEL IDRETRY microceller_process_retry
    ${EndIf}
    SetErrorLevel 2
    Quit
    microceller_process_retry:
  ${Loop}
!macroend

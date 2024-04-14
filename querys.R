library(DBI)
library(RSQLite)
#instalar stringr con comando:i install.packages("stringr")
library("stringr")
library(dplyr)

#validar si existe archivo de bd
file.exists("prueba_adres_2/db/dbadres.sqlite")

# Crear conexion con driver de SQLIte y la base de datos
con<-dbConnect(RSQLite::SQLite(), "prueba_adres_2/db/dbadres.sqlite")

# Comprobar conexion si existen las tablas
dbListTables(con)

# Querys para obtener datos de las dos tablas: Municipios y Prestadores
dfMunicipios<-dbGetQuery(con,  "SELECT * FROM Municipios")
# Cast de columna en query para corregir error de columna codigo_habilitacion--error mixed type. Existen datos tipo string.
dfPrestadores<-dbGetQuery(con,  "SELECT depa_nombre,  muni_nombre, cast(codigo_habilitacion as TEXT) AS codigo_habilitacion, nombre_prestador, tido_codigo, nits_nit, razon_social, clpr_codigo, clpr_nombre, ese, direccion, telefono, fax, email, gerente, nivel, caracter, habilitado, fecha_radicacion, fecha_vencimiento, fecha_cierre, dv, clase_persona, naju_codigo, naju_nombre, numero_sede_principal, fecha_corte_REPS, telefono_adicional, email_adicional, rep_legal FROM Prestadores")

# Eliminar espacios entre palabras, reemplazar alfanumericos y letra U mayuscula por u minuscula en columna Departamento
dfMunicipios$Departamento <- str_replace_all(dfMunicipios$Departamento, "[^[:alnum:]]", "")
dfMunicipios$Departamento <- str_replace_all(dfMunicipios$Departamento, "U", "u")
dfMunicipios$Departamento <- str_replace_all(dfMunicipios$Departamento, "(de|del)([A-Z])", " \\1 \\2")
dfMunicipios$Departamento <- str_replace_all(dfMunicipios$Departamento, "(\\p{Ll})(\\p{Lu})", "\\1 \\2")

# Eliminar espacios entre palabras, reemplazar alfanumericos y letra U mayuscula por u minuscula en columna Municipio
dfMunicipios$Municipio <- str_replace_all(dfMunicipios$Municipio, "[^[:alnum:]]", "")
dfMunicipios$Municipio <- str_replace_all(dfMunicipios$Municipio, "U", "u")
dfMunicipios$Municipio <- str_replace_all(dfMunicipios$Municipio, "(de|del)([A-Z])", " \\1 \\2")
dfMunicipios$Municipio <- str_replace_all(dfMunicipios$Municipio, "(\\p{Ll})(\\p{Lu})", "\\1 \\2")

# Formatear texto de los campos de la tabla Prestadores
dfPrestadores$muni_nombre <- str_to_title(dfPrestadores$muni_nombre)
dfPrestadores$nombre_prestador <- str_to_upper(dfPrestadores$nombre_prestador)
dfPrestadores$razon_social <- str_to_upper(dfPrestadores$razon_social)
dfPrestadores$direccion <- str_to_title(dfPrestadores$direccion)
dfPrestadores$rep_legal <-  str_to_title(dfPrestadores$rep_legal)
dfPrestadores$email <-  str_to_lower(dfPrestadores$email)

# solucionar problema con columna codigo_habilitacion
# Reemplazar la columna codigo_habilitacion en dos filas donde existe un espacio
dfPrestadores$codigo_habilitacion <- str_replace_all(dfPrestadores$codigo_habilitacion, " ", "0")
dfPrestadores$codigo_habilitacion <- str_replace_all(dfPrestadores$codigo_habilitacion, " ", "0")

#Cambiar columna de codigo habilitacion a numerico
dfPrestadores$codigo_habilitacion <- as.numeric(dfPrestadores$codigo_habilitacion)

# Convertir la cadena a una fecha en columnas fecha_radicacion y fecha_vencimiento
dfPrestadores$fecha_radicacion <- as.Date(strptime(dfPrestadores$fecha_radicacion, "%Y%m%d", tz = "UCT"), "%Y-%m-%d")
dfPrestadores$fecha_vencimiento <- as.Date(strptime(dfPrestadores$fecha_vencimiento, "%Y%m%d", tz = "UCT"), "%Y-%m-%d")

# Convertir la cadena a una fecha en la columna de fecha_corte_REPS
dfPrestadores$fecha_corte_REPS <- sub(".*: ", "", dfPrestadores$fecha_corte_REPS)
dfPrestadores$fecha_corte_REPS <- as.Date(dfPrestadores$fecha_corte_REPS, format = "%b %d %Y %I:%M%p")

#Concatenar columnas para crear llave primaria de Departamento_Municipio
dfPrestadores$keyDepMun <- paste(dfPrestadores$depa_nombre, dfPrestadores$muni_nombre, sep = "_")

#Concatenar columnas para crear llave primaria de Departamento_Municipio
dfMunicipios$keyDepMun <- paste(dfMunicipios$Departamento, dfMunicipios$Municipio, sep = "_")

# Desnormarlinazar: Unir datos de las tablas, manteniendo la tabla de la izquierda (dfPrestadores)
# Unión izquierda
df_merge <- merge(dfPrestadores, dfMunicipios, by = "keyDepMun", all.x = TRUE)




### Estadisticas

# Cantidad de Prestadores por CLPR 
#Recuento de filas por clpr_nombre
mi_tabla <- df_merge %>%
  group_by(df_merge$clpr_nombre) %>%
  summarise(cantidad_clpr = n())



# Cantidad de Prestadores por Region
#Recuento de filas por Region
mi_tabla <- df_merge %>%
  group_by(df_merge$Region) %>%
  summarise(cantidad_region = n())

# Cantidad de Prestadores por Departamento
#Recuento de filas por Departamento
mi_tabla <- df_merge %>%
  group_by(df_merge$Departamento) %>%
  summarise(cantidad_dpto = n())

# Cantidad de Prestadores por Municipio
#Recuento de filas por Municipio
df_merge %>%
  group_by(df_merge$Municipio) %>%
  summarise(cantidad_mnpo = n())

# Cantidad de Prestadores habilitados
#Recuento de filas por Municipio
mi_tabla <- df_merge %>%
  group_by(df_merge$habilitado) %>%
  summarise(cantidad_habilitado = n())


# Cantidad de Tipo de entidad
#Recuento de filas por Municipio
df_merge %>%
  group_by(df_merge$naju_nombre) %>%
  summarise(cantidad_naju = n())


#Estadisticas basadas en diasVigencia
# Calcular los años habilitados de los prestadores
df_merge$aniosHabiitados <- as.numeric(difftime(df_merge$fecha_vencimiento, df_merge$fecha_radicacion)) / 365.25

# Calcular los dias por vencer
df_merge$diasVigencia = as.numeric(difftime(df_merge$fecha_vencimiento, Sys.Date(), units = "days"))

# Clasificar los prestadores vencidos o vigentes segun fecha de radicacion (VENCIDO O VIGENTE)
df_merge$radicadoVencido <- ifelse(df_merge$diasVigencia <= 0, "VENCIDO", "VIGENTE")

# Clasificar los prestadores que estan por vencer (POR VENCER O A TIEMPO)
df_merge$radicadoPorvencer <- ifelse(df_merge$diasVigencia <= 15 & df_merge$diasVigencia > 0, "POR VENCER", "A TIEMPO")

# Agrupar por Departamento y radicadoVencido para obtener recuento de filas
mi_tabla <- df_merge %>%
  group_by(df_merge$Departamento, df_merge$radicadoVencido) %>%
  summarise(cant_estadoVig = n(), .groups = 'drop')

# Agrupar por Municipio y radicadoVencido para obtener recuento 
mi_tabla <- df_merge %>%
  group_by(df_merge$Municipio, df_merge$radicadoVencido) %>%
  summarise(cant_estadoVig = n(), .groups = 'drop')

# Agrupar por tipo Entidad y radicadoVencido para obtener recuento 
mi_tabla <- df_merge %>%
  group_by(df_merge$naju_nombre, df_merge$radicadoVencido) %>%
  summarise(cant_estadoVig = n(), .groups = 'drop')

# Distribucion normal de los datos por dias vencidos
df_merge$diasVigencia_normalizada <- scale(df_merge$diasVigencia)



### Graficas




# Distribucion de dias de vigencia
# 1. Cargar tus datos (reemplaza "mi_df" y "mi_columna" con los nombres reales)
mi_media <- mean(df_merge$diasVigencia)
mi_desviacion <- sd(df_merge$diasVigencia)
# 2. Crear una rejilla para el eje X
mi_rejilla <- seq(min(df_merge$diasVigencia), max(df_merge$diasVigencia), length.out = 100)
# 3. Calcular la función de densidad normal
mi_densidad <- dnorm(mi_rejilla, mean = mi_media, sd = mi_desviacion)
# 4. Graficar la distribución normal
plot(mi_rejilla, mi_densidad, type = "l", xlab = "Dias de vigencia", ylab = "Densidad")

# Graficon de Frecuencia con datos de días vigencia
hist(df_merge$diasVigencia, breaks = 20,
     main = "Histograma de Dias de vigencia",
     xlab = "Dias de vigencia", ylab = "Frecuencia",
     col = "lightblue")


# Graficar la dispersion y regresion lineal entre la superficie del municipio vs dias de vigencia
lmSuperficie = lm(as.integer(df_merge$Superficie) ~ df_merge$diasVigencia, data = df_merge)
plot( df_merge$Superficie, df_merge$diasVigencia , main = "Diagrama de Dispersión: Superfice vs. Dias Vigencia", xlab = "Superficie", ylab = "dias Vigencia")
abline(lmSuperficie, col = "red")
summary(lmSuperficie)

#Desactivar conexion
desconectado<-dbDisconnect(con)
desconectado

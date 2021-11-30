project(normal-catalogue VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-catalogue STATIC
        include/normal/catalogue/Globals.h
        include/normal/catalogue/CatalogueEntryType.h
        src/Catalogue.cpp include/normal/catalogue/Catalogue.h
        src/CatalogueEntry.cpp include/normal/catalogue/CatalogueEntry.h
        src/Table.cpp include/normal/catalogue/Table.h
        src/Partition.cpp include/normal/catalogue/Partition.h
        src/s3/S3CatalogueEntry.cpp include/normal/catalogue/s3/S3CatalogueEntry.h
        src/s3/S3Table.cpp include/normal/catalogue/s3/S3Table.h
        src/s3/S3Partition.cpp include/normal/catalogue/s3/S3Partition.h
        src/s3/S3CatalogueEntryReader.cpp include/normal/catalogue/s3/S3CatalogueEntryReader.h
        src/local-fs/LocalFSCatalogueEntry.cpp include/normal/catalogue/local-fs/LocalFSCatalogueEntry.h
        src/local-fs/LocalFSTable.cpp include/normal/catalogue/local-fs/LocalFSTable.h
        src/local-fs/LocalFSPartition.cpp include/normal/catalogue/local-fs/LocalFSPartition.h
        include/normal/catalogue/format/FormatType.h
        src/format/Format.cpp include/normal/catalogue/format/Format.h
        src/format/CSVFormat.cpp include/normal/catalogue/format/CSVFormat.h
        src/format/ParquetFormat.cpp include/normal/catalogue/format/ParquetFormat.h)


target_include_directories(normal-catalogue PUBLIC include)

target_link_libraries(normal-catalogue PUBLIC nlohmann_json)
target_link_libraries(normal-catalogue PUBLIC normal-tuple)
target_link_libraries(normal-catalogue PUBLIC normal-aws)
target_link_libraries(normal-catalogue PUBLIC normal-util)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-catalogue)

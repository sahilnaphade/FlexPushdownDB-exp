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
        src/s3/S3Util.cpp include/normal/catalogue/s3/S3Util.h
        src/local-fs/LocalFSCatalogueEntry.cpp include/normal/catalogue/local-fs/LocalFSCatalogueEntry.h
        src/local-fs/LocalFSTable.cpp include/normal/catalogue/local-fs/LocalFSTable.h
        src/local-fs/LocalFSPartition.cpp include/normal/catalogue/local-fs/LocalFSPartition.h)

#add_library(normal-catalogue STATIC
#        include/normal/connector/Globals.h
#        src/Connector.cpp ATTIC/Connector.h
#        src/CatalogueEntry.cpp include/normal/connector/CatalogueEntry.h
#        src/Catalogue.cpp include/normal/connector/Catalogue.h
#        src/s3/S3SelectConnector.cpp include/normal/connector/s3/S3SelectConnector.h
#        src/s3/S3SelectCatalogueEntry.cpp include/normal/connector/s3/S3SelectCatalogueEntry.h
#        src/local-fs/LocalFileSystemCatalogueEntry.cpp include/normal/connector/local-fs/LocalFileSystemCatalogueEntry.h
#        src/local-fs/LocalFileSystemConnector.cpp include/normal/connector/local-fs/LocalFileSystemConnector.h
#        src/local-fs/LocalFilePartitioningScheme.cpp include/normal/connector/local-fs/LocalFilePartitioningScheme.h
#        src/local-fs/LocalFileExplicitPartitioningScheme.cpp include/normal/connector/local-fs/LocalFileExplicitPartitioningScheme.h
#        src/local-fs/LocalFilePartition.cpp include/normal/connector/local-fs/LocalFilePartition.h
#        src/partition/PartitioningScheme.cpp include/normal/connector/partition/PartitioningScheme.h
#        src/partition/Partition.cpp include/normal/connector/partition/Partition.h
#        src/s3/S3SelectPartition.cpp include/normal/connector/s3/S3SelectPartition.h
#        src/s3/S3SelectPartitioningScheme.cpp include/normal/connector/s3/S3SelectPartitioningScheme.h
#        src/s3/S3SelectExplicitPartitioningScheme.cpp include/normal/connector/s3/S3SelectExplicitPartitioningScheme.h
#        src/s3/S3Util.cpp include/normal/connector/s3/S3Util.h)
#        src/MiniCatalogue.cpp include/normal/connector/MiniCatalogue.h)

target_include_directories(normal-catalogue PUBLIC include)

target_link_libraries(normal-catalogue PUBLIC aws-cpp-sdk-s3)
target_link_libraries(normal-catalogue PUBLIC spdlog::spdlog)
target_link_libraries(normal-catalogue PUBLIC fmt::fmt)
target_link_libraries(normal-catalogue PUBLIC expected)
target_link_libraries(normal-catalogue PUBLIC normal-expression-gandiva)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-catalogue)

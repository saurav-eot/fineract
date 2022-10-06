/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.infrastructure.dataqueries.service;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.persistence.PersistenceException;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.fineract.infrastructure.codes.service.CodeReadPlatformService;
import org.apache.fineract.infrastructure.configuration.domain.ConfigurationDomainService;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.data.ApiParameterError;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResult;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResultBuilder;
import org.apache.fineract.infrastructure.core.data.DataValidatorBuilder;
import org.apache.fineract.infrastructure.core.exception.GeneralPlatformDomainRuleException;
import org.apache.fineract.infrastructure.core.exception.PlatformApiDataValidationException;
import org.apache.fineract.infrastructure.core.exception.PlatformDataIntegrityException;
import org.apache.fineract.infrastructure.core.exception.PlatformServiceUnavailableException;
import org.apache.fineract.infrastructure.core.serialization.DatatableCommandFromApiJsonDeserializer;
import org.apache.fineract.infrastructure.core.serialization.FromJsonHelper;
import org.apache.fineract.infrastructure.core.serialization.JsonParserHelper;
import org.apache.fineract.infrastructure.core.service.database.DatabaseSpecificSQLGenerator;
import org.apache.fineract.infrastructure.core.service.database.DatabaseTypeResolver;
import org.apache.fineract.infrastructure.dataqueries.api.DataTableApiConstant;
import org.apache.fineract.infrastructure.dataqueries.data.DataTableValidator;
import org.apache.fineract.infrastructure.dataqueries.data.DatatableData;
import org.apache.fineract.infrastructure.dataqueries.data.GenericResultsetData;
import org.apache.fineract.infrastructure.dataqueries.data.ResultsetColumnHeaderData;
import org.apache.fineract.infrastructure.dataqueries.data.ResultsetRowData;
import org.apache.fineract.infrastructure.dataqueries.exception.DatatableEntryRequiredException;
import org.apache.fineract.infrastructure.dataqueries.exception.DatatableNotFoundException;
import org.apache.fineract.infrastructure.dataqueries.exception.DatatableSystemErrorException;
import org.apache.fineract.infrastructure.security.service.PlatformSecurityContext;
import org.apache.fineract.infrastructure.security.service.SqlInjectionPreventerService;
import org.apache.fineract.infrastructure.security.utils.ColumnValidator;
import org.apache.fineract.infrastructure.security.utils.SQLInjectionValidator;
import org.apache.fineract.useradministration.domain.AppUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ReadWriteNonCoreDataServiceImpl implements ReadWriteNonCoreDataService {

    public static final String STRING = "string";
    public static final String SELECT_APPLICATION_TABLE_NAME_FROM_X_REGISTERED_TABLE_WHERE_REGISTERED_TABLE_NAME = "SELECT application_table_name FROM x_registered_table where registered_table_name = ?";
    public static final String OFFICE_ID = "officeId";
    public static final String GROUP_ID = "groupId";
    public static final String CLIENT_ID = "clientId";
    public static final String SAVINGS_ID = "savingsId";
    public static final String LOAN_ID = "loanId";
    public static final String ENTITY_ID = "entityId";
    public static final String DATA_SCOPED_SQL_FOR_LOGGING = "data scoped sql: {}";
    public static final String SYSTEM_ERROR_MORE_THAN_ONE_ROW_RETURNED_FROM_DATA_SCOPING_QUERY = "System Error: More than one row returned from data scoping query";
    public static final String ID = "id";
    public static final String ID_POSTFIX = "_id";
    public static final String SINGLE_QUOTE = "'";
    public static final String EMPTY_STRING = "";
    public static final String NULL = "null";
    public static final String TRUE_AS_NUMBER_STRING = "1";
    public static final String FALSE_AS_NUMBER_STRING = "0";
    public static final String B_1 = "B'1'";
    public static final String B_0 = "B'0'";
    public static final String AS = " as ";
    public static final String COLON_SPACE = ", ";
    public static final String INSERT_INTO = "insert into ";
    public static final String BRACKET_OPENING = "(";
    public static final String SPACE = " ";
    public static final String BRACKET_CLOSING = ")";
    public static final String PERCENTAGE_SIGN = "%";
    public static final String SAVINGS_DATA_SCOPED_SQL = "select distinct x.* from ((select o.id as officeId, s.group_id as groupId, s.client_id as clientId, s.id as savingsId, null as loanId, null as entityId from m_savings_account s join m_client c on c.id = s.client_id join m_office o on o.id = c.office_id and o.hierarchy like ? where s.id = ?"
            + BRACKET_CLOSING
            + " union all (select o.id as officeId, s.group_id as groupId, s.client_id as clientId, s.id as savingsId, null as loanId, null as entityId from m_savings_account s join m_group g on g.id = s.group_id join m_office o on o.id = g.office_id and o.hierarchy like ? where s.id = ?"
            + BRACKET_CLOSING + " ) as x";
    public static final String LOAN_DATA_SCOPED_SQL = "select distinct x.* from ((select o.id as officeId, l.group_id as groupId, l.client_id as clientId, null as savingsId, l.id as loanId, null as entityId from m_loan l join m_client c on c.id = l.client_id join m_office o on o.id = c.office_id and o.hierarchy like ? where l.id = ?"
            + BRACKET_CLOSING
            + " union all (select o.id as officeId, l.group_id as groupId, l.client_id as clientId, null as savingsId, l.id as loanId, null as entityId from m_loan l join m_group g on g.id = l.group_id join m_office o on o.id = g.office_id and o.hierarchy like ?where l.id = ?"
            + BRACKET_CLOSING + " ) as x";
    public static final String SELECT = " select ";
    public static final String AS_ID = " as id ";
    public static final String CURLY_BRACKETS = "{}";
    public static final String BIT = "bit";
    public static final String ORDER_BY_APPLICATION_TABLE_NAME_REGISTERED_TABLE_NAME = "order by application_table_name, registered_table_name";
    public static final String AND_APPLICATION_TABLE_NAME_LIKE = "and application_table_name like ?";
    public static final String PERMITTED_DATA_TABLES = "select application_table_name, registered_table_name, entity_subtype from x_registered_table where exists (select 'f' from m_appuser_role ur join m_role r on r.id = ur.role_id left join m_role_permission rp on rp.role_id = r.id left join m_permission p on p.id = rp.permission_id where ur.appuser_id = ? and (p.code in ('ALL_FUNCTIONS', 'ALL_FUNCTIONS_READ') or p.code = concat('READ_', registered_table_name)))";
    public static final String APPLICATION_TABLE_NAME_COLUMN = "application_table_name";
    public static final String REGISTERED_TABLE_NAME_COLUMN = "registered_table_name";
    public static final String ENTITY_SUBTYPE_COLUMN = "entity_subtype";
    public static final String PERMITTED_DATATABLES_WITH_ORDERING = "select application_table_name, registered_table_name, entity_subtype from x_registered_table where exists (select 'f' from m_appuser_role ur join m_role r on r.id = ur.role_id left join m_role_permission rp on rp.role_id = r.id left join m_permission p on p.id = rp.permission_id where ur.appuser_id = ? and registered_table_name=? and (p.code in ('ALL_FUNCTIONS', 'ALL_FUNCTIONS_READ') or p.code = concat('READ_', registered_table_name))) order by application_table_name, registered_table_name";
    public static final String ENTITY_SUB_TYPE = "entitySubType";
    public static final String CODE = "code";
    public static final String ENTITY_NAME = "entity_name";
    public static final String REGISTER_DATATABLE_SQL = "insert into x_registered_table (registered_table_name, application_table_name, entity_subtype, category) values ( :dataTableName, :applicationTableName, :entitySubType ,:category)";
    public static final String DATA_TABLE_NAME = "dataTableName";
    public static final String APPLICATION_TABLE_NAME = "applicationTableName";
    public static final String CATEGORY = "category";
    public static final String INSERT_INTO_C_CONFIGURATION_NAME_VALUE_ENABLED_VALUES_DATA_TABLE_NAME_0_FALSE = "insert into c_configuration (name, value, enabled ) values( :dataTableName , '0',false)";
    public static final String DUPLICATE_ENTRY = "Duplicate entry";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String LENGTH = "length";
    public static final String MANDATORY = "mandatory";
    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String SEPARATOR = "/";
    public static final String DATATABLE_REGISTERED_SQL = "select (CASE WHEN exists (select 1 from x_registered_table where registered_table_name = ?) THEN 'true' ELSE 'false' END)";
    public static final String DATATABLE_EXIST_SQL = "select (CASE WHEN exists (select 1 from information_schema.tables where table_schema = ? and table_name = ?) THEN 'true' ELSE 'false' END)";
    public static final String CD_INLINE_STR = "_cd_";
    public static final String UNDERSCORE = "_";
    public static final String FK_PREFIX = "fk_";
    public static final String CONSTRAINT = ", CONSTRAINT ";
    public static final String FOREIGN_KEY = "FOREIGN KEY (";
    public static final String REFERENCES_ = "REFERENCES ";
    public static final String ID1 = " (id)";
    public static final String DECIMAL = "Decimal";
    public static final String DROPDOWN = "Dropdown";
    public static final String DECIMAL_LENGTH = "(19,6)";
    public static final String DROPDOWN_LENGHT = "(11)";
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String VARCHAR = "varchar";
    public static final String NOT_NULL = " NOT NULL";
    public static final String DEFAULT_NULL = " DEFAULT NULL";
    public static final String COLUMNS = "columns";
    public static final String DATATABLE_NAME = "datatableName";
    public static final String APPTABLE_NAME = "apptableName";
    public static final String MULTI_ROW = "multiRow";
    public static final String NEW_NAME = "newName";
    public static final String AFTER = "after";
    public static final String NEW_CODE = "newCode";
    public static final String SPACE_REGEXP = "\\s";
    public static final String DROP_FOREIGN_KEY_CONSTRAINT = ", DROP FOREIGN KEY ";
    public static final String ADD_CONSTRAINT_SQL = ",ADD CONSTRAINT ";
    public static final String CHANGE_SQL = ", CHANGE ";
    public static final String AFTER_SQL = " AFTER ";
    public static final String SELECT_CODEID_FROM_MAPPINGS_SQL = "select ccm.code_id from x_table_column_code_mappings ccm where ccm.column_alias_name='";
    public static final String COLON_ADD = ", ADD ";
    public static final String UPDATE_SQL = "Update sql: {}";
    public static final String NO_CHANGES = "No Changes";
    public static final String SELECT_FROM = "select * from ";
    public static final String WHERE = " where ";
    public static final String AND_ID = " and id = ";
    public static final String ORDER_BY = " order by ";
    public static final String M_LOAN = "m_loan";
    public static final String M_SAVINGS_ACCOUNT = "m_savings_account";
    public static final String M_CLIENT = "m_client";
    public static final String M_GROUP = "m_group";
    public static final String M_CENTER = "m_center";
    public static final String M_OFFICE = "m_office";
    public static final String M_SAVINGS_PRODUCT = "m_savings_product";
    public static final String M_PRODUCT_LOAN = "m_product_loan";
    public static final String CLIENT_DATA_SCOPED_SQL = "select o.id as officeId, null as groupId, c.id as clientId, null as savingsId, null as loanId, null as entityId from m_client c join m_office o on o.id = c.office_id and o.hierarchy like ? where c.id = ?";
    public static final String GROUP_DATA_SCOPED_SQL = "select o.id as officeId, g.id as groupId, null as clientId, null as savingsId, null as loanId, null as entityId from m_group g join m_office o on o.id = g.office_id and o.hierarchy like ? where g.id = ?";
    public static final String OFFICE_DATA_SCOPED_SQL = "select o.id as officeId, null as groupId, null as clientId, null as savingsId, null as loanId, null as entityId from m_office o where o.hierarchy like ? and o.id = ?";
    public static final String PRODUCT_DATA_SCOPED_SQL = "select null as officeId, null as groupId, null as clientId, null as savingsId, null as loanId, p.id as entityId from ? as p WHERE p.id = ?";
    public static final String TIMESTAMP_WITHOUT_TIME_ZONE = "TIMESTAMP WITHOUT TIME ZONE";
    public static final String SPACE_COLON_DOLLAR = " ,$";
    public static final String UPDATE = "update ";
    public static final String SET = " set ";
    public static final String EQUALS = " = ";
    public static final String DATE_FORMAT = "dateFormat";
    public static final String LOCALE = "locale";
    public static final String DELETE_FROM = "delete from ";
    private static final String DATATABLE_NAME_REGEX_PATTERN = "^[a-zA-Z][a-zA-Z0-9\\-_\\s]{0,48}[a-zA-Z0-9]$";
    private static final String CODE_VALUES_TABLE = "m_code_value";
    private static final Logger LOG = LoggerFactory.getLogger(ReadWriteNonCoreDataServiceImpl.class);
    // TODO: Extract these types out of here
    private static final ImmutableMap<String, String> apiTypeToMySQL = ImmutableMap.<String, String>builder().put(STRING, "VARCHAR")
            .put("number", "INT").put("boolean", "BIT").put("decimal", "DECIMAL").put("date", DATE).put("datetime", DATETIME)
            .put("text", "TEXT").put("dropdown", "INT").build();
    private static final ImmutableMap<String, String> apiTypeToPostgreSQL = ImmutableMap.<String, String>builder().put(STRING, "VARCHAR")
            .put("number", "INT").put("boolean", "BIT").put("decimal", "DECIMAL").put("date", DATE).put("datetime", TIMESTAMP)
            .put("text", "TEXT").put("dropdown", "INT").build();
    private static final List<String> stringDataTypes = Arrays.asList("char", VARCHAR, "blob", "text", "tinyblob", "tinytext", "mediumblob",
            "mediumtext", "longblob", "longtext");
    public static final String EXIST_DATATABLE_ENTRIES_SQL = "SELECT COUNT(?) FROM ? WHERE ? = ?";
    public static final String IS_TRHERE_DATATABLE_CHECK_FOR_DATATABLE_SQL = "SELECT COUNT(edc.x_registered_table_name) FROM x_registered_table xrt JOIN m_entity_datatable_check edc ON edc.x_registered_table_name = xrt.registered_table_name WHERE edc.x_registered_table_name = ?";
    private final JdbcTemplate jdbcTemplate;
    private final DatabaseTypeResolver databaseTypeResolver;
    private final DatabaseSpecificSQLGenerator sqlGenerator;
    private final PlatformSecurityContext context;
    private final FromJsonHelper fromJsonHelper;
    private final JsonParserHelper helper = new JsonParserHelper();
    private final GenericDataService genericDataService;
    private final DatatableCommandFromApiJsonDeserializer fromApiJsonDeserializer;
    private final ConfigurationDomainService configurationDomainService;
    private final CodeReadPlatformService codeReadPlatformService;
    private final DataTableValidator dataTableValidator;
    private final ColumnValidator columnValidator;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final SqlInjectionPreventerService preventSqlInjectionService;

    @Override
    public List<DatatableData> retrieveDatatableNames(final String appTable) {
        Object[] params = new Object[] { this.context.authenticatedUser().getId() };
        // PERMITTED datatables
        String sql = PERMITTED_DATA_TABLES;
        if (appTable != null) {
            sql = sql + SPACE + AND_APPLICATION_TABLE_NAME_LIKE + SPACE;
            params = new Object[] { this.context.authenticatedUser().getId(), appTable };
        }
        sql = sql + SPACE + ORDER_BY_APPLICATION_TABLE_NAME_REGISTERED_TABLE_NAME;

        final List<DatatableData> datatables = new ArrayList<>();

        final SqlRowSet rowSet = jdbcTemplate.queryForRowSet(sql, params); // NOSONAR
        while (rowSet.next()) {
            final String appTableName = rowSet.getString(APPLICATION_TABLE_NAME_COLUMN);
            final String registeredDatatableName = rowSet.getString(REGISTERED_TABLE_NAME_COLUMN);
            final String entitySubType = rowSet.getString(ENTITY_SUBTYPE_COLUMN);
            final List<ResultsetColumnHeaderData> columnHeaderData = genericDataService.fillResultsetColumnHeaders(registeredDatatableName);

            datatables.add(DatatableData.create(appTableName, registeredDatatableName, entitySubType, columnHeaderData));
        }

        return datatables;
    }

    @Override
    public DatatableData retrieveDatatable(final String datatable) {

        // PERMITTED datatables
        SQLInjectionValidator.validateSQLInput(datatable);

        DatatableData datatableData = null;

        final SqlRowSet rowSet = jdbcTemplate.queryForRowSet(PERMITTED_DATATABLES_WITH_ORDERING, this.context.authenticatedUser().getId(),
                datatable); // NOSONAR
        if (rowSet.next()) {
            final String appTableName = rowSet.getString(APPLICATION_TABLE_NAME_COLUMN);
            final String registeredDatatableName = rowSet.getString(REGISTERED_TABLE_NAME_COLUMN);
            final String entitySubType = rowSet.getString(ENTITY_SUBTYPE_COLUMN);
            final List<ResultsetColumnHeaderData> columnHeaderData = this.genericDataService
                    .fillResultsetColumnHeaders(registeredDatatableName);

            datatableData = DatatableData.create(appTableName, registeredDatatableName, entitySubType, columnHeaderData);
        }

        return datatableData;
    }

    private void logAsErrorUnexpectedDataIntegrityException(final Exception dve) {
        LOG.error("Error occurred.", dve);
    }

    @Transactional
    @Override
    public void registerDatatable(final String dataTableName, final String applicationTableName, final String entitySubType) {

        Integer category = DataTableApiConstant.CATEGORY_DEFAULT;

        final String permissionSql = this.getPermissionSql(dataTableName);
        this.registerDataTable(applicationTableName, dataTableName, entitySubType, category, permissionSql);

    }

    @Transactional
    @Override
    public void registerDatatable(final JsonCommand command) {

        final String applicationTableName = this.getTableName(command.getUrl());
        final String dataTableName = this.getDataTableName(command.getUrl());
        final String entitySubType = command.stringValueOfParameterNamed(ENTITY_SUB_TYPE);

        Integer category = this.getCategory(command);

        this.dataTableValidator.validateDataTableRegistration(command.json());
        final String permissionSql = this.getPermissionSql(dataTableName);
        this.registerDataTable(applicationTableName, dataTableName, entitySubType, category, permissionSql);

    }

    @Transactional
    @Override
    public void registerDatatable(final JsonCommand command, final String permissionSql) {
        final String applicationTableName = this.getTableName(command.getUrl());
        final String dataTableName = this.getDataTableName(command.getUrl());
        final String entitySubType = command.stringValueOfParameterNamed(ENTITY_SUB_TYPE);

        Integer category = this.getCategory(command);

        this.dataTableValidator.validateDataTableRegistration(command.json());

        this.registerDataTable(applicationTableName, dataTableName, entitySubType, category, permissionSql);

    }

    private void registerDataTable(final String applicationTableName, final String dataTableName, final String entitySubType,
            final Integer category, final String permissionsSql) {

        validateAppTable(applicationTableName);
        validateDatatableName(dataTableName);
        assertDataTableExists(dataTableName);

        Map<String, Object> paramMap = new HashMap<>(3);
        paramMap.put(DATA_TABLE_NAME, dataTableName);
        paramMap.put(APPLICATION_TABLE_NAME, applicationTableName);
        paramMap.put(ENTITY_SUB_TYPE, entitySubType);
        paramMap.put(CATEGORY, category);

        try {
            this.namedParameterJdbcTemplate.update(REGISTER_DATATABLE_SQL, paramMap);
            this.jdbcTemplate.update(permissionsSql);

            // add the registered table to the config if it is a ppi
            if (this.isSurveyCategory(category)) {
                this.namedParameterJdbcTemplate.update(INSERT_INTO_C_CONFIGURATION_NAME_VALUE_ENABLED_VALUES_DATA_TABLE_NAME_0_FALSE,
                        paramMap);
            }

        } catch (final JpaSystemException | DataIntegrityViolationException dve) {
            final Throwable cause = dve.getCause();
            final Throwable realCause = dve.getMostSpecificCause();
            // even if duplicate is only due to permission duplicate, okay to
            // show duplicate datatable error msg
            if (realCause.getMessage().contains(DUPLICATE_ENTRY) || cause.getMessage().contains(DUPLICATE_ENTRY)) {
                throw new PlatformDataIntegrityException("error.msg.datatable.registered",
                        "Datatable `" + dataTableName + "` is already registered against an application table.", DATA_TABLE_NAME,
                        dataTableName, dve);
            }
            logAsErrorUnexpectedDataIntegrityException(dve);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.", dve);
        } catch (final PersistenceException dve) {
            final Throwable cause = dve.getCause();
            if (cause.getMessage().contains(DUPLICATE_ENTRY)) {
                throw new PlatformDataIntegrityException("error.msg.datatable.registered",
                        "Datatable `" + dataTableName + "` is already registered against an application table.", DATA_TABLE_NAME,
                        dataTableName, dve);
            }
            logAsErrorUnexpectedDataIntegrityException(dve);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.", dve);
        }

    }

    private String getPermissionSql(final String dataTableName) {
        final String createPermission = "'CREATE_" + dataTableName + "'";
        final String createPermissionChecker = "'CREATE_" + dataTableName + "_CHECKER'";
        final String readPermission = "'READ_" + dataTableName + "'";
        final String updatePermission = "'UPDATE_" + dataTableName + "'";
        final String updatePermissionChecker = "'UPDATE_" + dataTableName + "_CHECKER'";
        final String deletePermission = "'DELETE_" + dataTableName + "'";
        final String deletePermissionChecker = "'DELETE_" + dataTableName + "_CHECKER'";
        final List<String> escapedColumns = Stream.of("grouping", "code", "action_name", "entity_name", "can_maker_checker")
                .map(sqlGenerator::escape).toList();
        final String columns = String.join(", ", escapedColumns);

        return "insert into m_permission (" + columns + ") values " + "('datatable', " + createPermission + ", 'CREATE', '" + dataTableName
                + "', true)," + "('datatable', " + createPermissionChecker + ", 'CREATE', '" + dataTableName + "', false),"
                + "('datatable', " + readPermission + ", 'READ', '" + dataTableName + "', false)," + "('datatable', " + updatePermission
                + ", 'UPDATE', '" + dataTableName + "', true)," + "('datatable', " + updatePermissionChecker + ", 'UPDATE', '"
                + dataTableName + "', false)," + "('datatable', " + deletePermission + ", 'DELETE', '" + dataTableName + "', true),"
                + "('datatable', " + deletePermissionChecker + ", 'DELETE', '" + dataTableName + "', false)";

    }

    private Integer getCategory(final JsonCommand command) {
        Integer category = command.integerValueOfParameterNamedDefaultToNullIfZero(DataTableApiConstant.categoryParamName);
        if (category == null) {
            category = DataTableApiConstant.CATEGORY_DEFAULT;
        }
        return category;
    }

    private boolean isSurveyCategory(final Integer category) {
        return category.equals(DataTableApiConstant.CATEGORY_PPI);
    }

    private JsonElement addColumn(final String name, final String dataType, final boolean isMandatory, final Integer length) {
        JsonObject column = new JsonObject();
        column.addProperty(NAME, name);
        column.addProperty(TYPE, dataType);
        if (dataType.equalsIgnoreCase(STRING)) {
            column.addProperty(LENGTH, length);
        }
        column.addProperty(MANDATORY, (isMandatory ? TRUE : FALSE));
        return column;
    }

    @Override
    public String getDataTableName(String url) {

        List<String> urlParts = Splitter.on(SEPARATOR).splitToList(url);

        return urlParts.get(3);

    }

    @Override
    public String getTableName(String url) {
        List<String> urlParts = Splitter.on(SEPARATOR).splitToList(url);
        return urlParts.get(4);
    }

    @Transactional
    @Override
    public void deregisterDatatable(final String datatable) {
        String validatedDatatable = this.preventSqlInjectionService.encodeSql(datatable);
        final String permissionList = "('CREATE_" + validatedDatatable + "', 'CREATE_" + validatedDatatable + "_CHECKER', 'READ_"
                + validatedDatatable + "', 'UPDATE_" + validatedDatatable + "', 'UPDATE_" + validatedDatatable + "_CHECKER', 'DELETE_"
                + validatedDatatable + "', 'DELETE_" + validatedDatatable + "_CHECKER')";

        final String deleteRolePermissionsSql = "delete from m_role_permission where m_role_permission.permission_id in (select id from m_permission where code in "
                + permissionList + BRACKET_CLOSING;

        final String deletePermissionsSql = "delete from m_permission where code in " + permissionList;

        final String deleteRegisteredDatatableSql = "delete from x_registered_table where registered_table_name = '" + validatedDatatable
                + SINGLE_QUOTE;

        final String deleteFromConfigurationSql = "delete from c_configuration where name ='" + validatedDatatable + SINGLE_QUOTE;

        String[] sqlArray = new String[4];
        sqlArray[0] = deleteRolePermissionsSql;
        sqlArray[1] = deletePermissionsSql;
        sqlArray[2] = deleteRegisteredDatatableSql;
        sqlArray[3] = deleteFromConfigurationSql;

        this.jdbcTemplate.batchUpdate(sqlArray); // NOSONAR
    }

    @Transactional
    @Override
    public CommandProcessingResult createNewDatatableEntry(final String dataTableName, final Long appTableId, final JsonCommand command) {
        return createNewDatatableEntry(dataTableName, appTableId, command.json());
    }

    @Transactional
    @Override
    public CommandProcessingResult createNewDatatableEntry(final String dataTableName, final Long appTableId, final String json) {
        try {
            final String appTable = queryForApplicationTableName(dataTableName);
            CommandProcessingResult commandProcessingResult = checkMainResourceExistsWithinScope(appTable, appTableId);

            final List<ResultsetColumnHeaderData> columnHeaders = this.genericDataService.fillResultsetColumnHeaders(dataTableName);

            final boolean multiRow = isMultirowDatatable(columnHeaders);

            final Type typeOfMap = new TypeToken<Map<String, String>>() {

            }.getType();
            final Map<String, String> dataParams = this.fromJsonHelper.extractDataMap(typeOfMap, json);

            final String sql = getAddSql(columnHeaders, dataTableName, getFKField(appTable), appTableId, dataParams);

            if (!multiRow) {
                this.jdbcTemplate.update(sql);
                commandProcessingResult = CommandProcessingResult.fromCommandProcessingResult(commandProcessingResult, appTableId);
            } else {
                final Long resourceId = addMultirowRecord(sql);
                commandProcessingResult = CommandProcessingResult.fromCommandProcessingResult(commandProcessingResult, resourceId);
            }

            return commandProcessingResult; //

        } catch (final SQLException e) {
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.", e);
        } catch (final DataAccessException dve) {
            final Throwable cause = dve.getCause();
            final Throwable realCause = dve.getMostSpecificCause();
            if (realCause.getMessage().contains(DUPLICATE_ENTRY) || cause.getMessage().contains(DUPLICATE_ENTRY)) {
                throw new PlatformDataIntegrityException(
                        "error.msg.datatable.entry.duplicate", "An entry already exists for datatable `" + dataTableName
                                + "` and application table with identifier `" + appTableId + "`.",
                        DATA_TABLE_NAME, dataTableName, appTableId, dve);
            } else if (realCause.getMessage().contains("doesn't have a default value")
                    || cause.getMessage().contains("doesn't have a default value")) {
                throw new PlatformDataIntegrityException(
                        "error.msg.datatable.no.value.provided.for.required.fields", "No values provided for the datatable `"
                                + dataTableName + "` and application table with identifier `" + appTableId + "`.",
                        DATA_TABLE_NAME, dataTableName, appTableId, dve);
            }

            logAsErrorUnexpectedDataIntegrityException(dve);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.", dve);
        } catch (final PersistenceException e) {
            final Throwable cause = e.getCause();
            if (cause.getMessage().contains(DUPLICATE_ENTRY)) {
                throw new PlatformDataIntegrityException(
                        "error.msg.datatable.entry.duplicate", "An entry already exists for datatable `" + dataTableName
                                + "` and application table with identifier `" + appTableId + "`.",
                        DATA_TABLE_NAME, dataTableName, appTableId, e);
            } else if (cause.getMessage().contains("doesn't have a default value")) {
                throw new PlatformDataIntegrityException(
                        "error.msg.datatable.no.value.provided.for.required.fields", "No values provided for the datatable `"
                                + dataTableName + "` and application table with identifier `" + appTableId + "`.",
                        DATA_TABLE_NAME, dataTableName, appTableId, e);
            }

            logAsErrorUnexpectedDataIntegrityException(e);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.", e);
        }
    }

    @Override
    public CommandProcessingResult createPPIEntry(final String dataTableName, final Long appTableId, final JsonCommand command) {

        try {
            final String appTable = queryForApplicationTableName(dataTableName);
            final CommandProcessingResult commandProcessingResult = checkMainResourceExistsWithinScope(appTable, appTableId);

            final List<ResultsetColumnHeaderData> columnHeaders = this.genericDataService.fillResultsetColumnHeaders(dataTableName);

            final Type typeOfMap = new TypeToken<Map<String, String>>() {

            }.getType();
            final Map<String, String> dataParams = this.fromJsonHelper.extractDataMap(typeOfMap, command.json());

            final String sql = getAddSqlWithScore(columnHeaders, dataTableName, getFKField(appTable), appTableId, dataParams);

            this.jdbcTemplate.update(sql);

            return commandProcessingResult; //

        } catch (final DataAccessException dve) {
            final Throwable cause = dve.getCause();
            final Throwable realCause = dve.getMostSpecificCause();
            if (realCause.getMessage().contains(DUPLICATE_ENTRY) || cause.getMessage().contains(DUPLICATE_ENTRY)) {
                throw new PlatformDataIntegrityException(
                        "error.msg.datatable.entry.duplicate", "An entry already exists for datatable `" + dataTableName
                                + "` and application table with identifier `" + appTableId + "`.",
                        DATA_TABLE_NAME, dataTableName, appTableId, dve);
            }

            logAsErrorUnexpectedDataIntegrityException(dve);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.", dve);
        } catch (final PersistenceException dve) {
            final Throwable cause = dve.getCause();
            if (cause.getMessage().contains(DUPLICATE_ENTRY)) {
                throw new PlatformDataIntegrityException(
                        "error.msg.datatable.entry.duplicate", "An entry already exists for datatable `" + dataTableName
                                + "` and application table with identifier `" + appTableId + "`.",
                        DATA_TABLE_NAME, dataTableName, appTableId, dve);
            }

            logAsErrorUnexpectedDataIntegrityException(dve);
            throw new PlatformDataIntegrityException("error.msg.unknown.data.integrity.issue",
                    "Unknown data integrity issue with resource.", dve);
        }
    }

    private boolean isRegisteredDataTable(final String name) {
        // PERMITTED datatables
        return Boolean.parseBoolean(this.jdbcTemplate.queryForObject(DATATABLE_REGISTERED_SQL, String.class, name));
    }

    private void assertDataTableExists(final String datatableName) {
        final boolean dataTableExists = Boolean.parseBoolean(
                this.jdbcTemplate.queryForObject(DATATABLE_EXIST_SQL, String.class, sqlGenerator.currentSchema(), datatableName));// NOSONAR
        if (!dataTableExists) {
            throw new PlatformDataIntegrityException("error.msg.invalid.datatable", "Invalid Data Table: " + datatableName, NAME,
                    datatableName);
        }
    }

    private void validateDatatableName(final String name) {

        if (name == null || name.isEmpty()) {
            throw new PlatformDataIntegrityException("error.msg.datatables.datatable.null.name", "Data table name must not be blank.");
        } else if (!name.matches(DATATABLE_NAME_REGEX_PATTERN)) {
            throw new PlatformDataIntegrityException("error.msg.datatables.datatable.invalid.name.regex", "Invalid data table name.", name);
        }
        SQLInjectionValidator.validateSQLInput(name);
    }

    private String datatableColumnNameToCodeValueName(final String columnName, final String code) {

        return code + CD_INLINE_STR + columnName;
    }

    private void throwExceptionIfValidationWarningsExist(final List<ApiParameterError> dataValidationErrors) {
        if (!dataValidationErrors.isEmpty()) {
            throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                    dataValidationErrors);
        }
    }

    private void parseDatatableColumnObjectForCreate(final JsonObject column, StringBuilder sqlBuilder,
            final StringBuilder constrainBuilder, final String dataTableNameAlias, final Map<String, Long> codeMappings,
            final boolean isConstraintApproach) {

        String name = column.has(NAME) ? column.get(NAME).getAsString() : null;
        final String type = column.has(TYPE) ? column.get(TYPE).getAsString().toLowerCase() : null;
        final Integer length = column.has(LENGTH) ? column.get(LENGTH).getAsInt() : null;
        final boolean mandatory = column.has(MANDATORY) && column.get(MANDATORY).getAsBoolean();
        final String code = column.has(CODE) ? column.get(CODE).getAsString() : null;

        if (StringUtils.isNotBlank(code)) {
            if (isConstraintApproach) {
                codeMappings.put(dataTableNameAlias + UNDERSCORE + name, this.codeReadPlatformService.retriveCode(code).getCodeId());
                String fkName = FK_PREFIX + dataTableNameAlias + UNDERSCORE + name;
                constrainBuilder.append(CONSTRAINT).append(sqlGenerator.escape(fkName)).append(SPACE).append(FOREIGN_KEY)
                        .append(sqlGenerator.escape(name)).append(BRACKET_CLOSING).append(SPACE).append(REFERENCES_)
                        .append(sqlGenerator.escape(CODE_VALUES_TABLE)).append(ID1);
            } else {
                name = datatableColumnNameToCodeValueName(name, code);
            }
        }

        final String dataType;
        if (databaseTypeResolver.isMySQL()) {
            dataType = apiTypeToMySQL.get(type);
        } else if (databaseTypeResolver.isPostgreSQL()) {
            dataType = apiTypeToPostgreSQL.get(type);
        } else {
            throw new IllegalStateException("Current database is not supported");
        }
        sqlBuilder.append(sqlGenerator.escape(name)).append(SPACE).append(dataType);

        if (type != null) {
            if (type.equalsIgnoreCase(STRING)) {
                sqlBuilder.append(BRACKET_OPENING).append(length).append(BRACKET_CLOSING);
            } else if (type.equalsIgnoreCase(DECIMAL)) {
                sqlBuilder.append(DECIMAL_LENGTH);
            } else if (type.equalsIgnoreCase(DROPDOWN)) {
                if (databaseTypeResolver.isMySQL()) {
                    sqlBuilder.append(DROPDOWN_LENGHT);
                }
            }
        }

        if (mandatory) {
            sqlBuilder.append(NOT_NULL);
        } else {
            sqlBuilder.append(DEFAULT_NULL);
        }

        sqlBuilder.append(COLON_SPACE);
    }

    @Transactional
    @Override
    public CommandProcessingResult createDatatable(final JsonCommand command) {

        String datatableName = null;

        try {
            this.context.authenticatedUser();
            this.fromApiJsonDeserializer.validateForCreate(command.json());

            final JsonElement element = this.fromJsonHelper.parse(command.json());
            final JsonArray columns = this.fromJsonHelper.extractJsonArrayNamed(COLUMNS, element);
            datatableName = this.fromJsonHelper.extractStringNamed(DATATABLE_NAME, element);
            String entitySubType = this.fromJsonHelper.extractStringNamed(ENTITY_SUB_TYPE, element);
            final String apptableName = this.fromJsonHelper.extractStringNamed(APPTABLE_NAME, element);
            Boolean multiRow = this.fromJsonHelper.extractBooleanNamed(MULTI_ROW, element);

            /***
             * In cases of tables storing hierarchical entities (like m_group), different entities would end up being
             * stored in the same table.
             *
             * Ex: Centers are a specific type of group, add abstractions for the same
             ***/
            final String actualAppTableName = mapToActualAppTable(apptableName);

            if (multiRow == null) {
                multiRow = false;
            }

            validateDatatableName(datatableName);
            validateAppTable(apptableName);
            final boolean isConstraintApproach = this.configurationDomainService.isConstraintApproachEnabledForDatatables();
            final String fkColumnName = apptableName.substring(2) + ID_POSTFIX;
            final String dataTableNameAlias = datatableName.toLowerCase().replaceAll(SPACE_REGEXP, UNDERSCORE);
            final String fkName = dataTableNameAlias + UNDERSCORE + fkColumnName;
            StringBuilder sqlBuilder = new StringBuilder();
            final StringBuilder constrainBuilder = new StringBuilder();
            final Map<String, Long> codeMappings = new HashMap<>();
            sqlBuilder.append("CREATE TABLE ").append(sqlGenerator.escape(datatableName)).append(SPACE).append(BRACKET_OPENING);

            if (multiRow) {
                if (databaseTypeResolver.isMySQL()) {
                    sqlBuilder.append("id BIGINT NOT NULL AUTO_INCREMENT, ");
                } else if (databaseTypeResolver.isPostgreSQL()) {
                    sqlBuilder.append(
                            "id bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ), ");
                } else {
                    throw new IllegalStateException("Current database is not supported");
                }
            }
            sqlBuilder.append(sqlGenerator.escape(fkColumnName)).append(" BIGINT NOT NULL, ");

            // Add Created At and Updated At
            columns.add(addColumn(DataTableApiConstant.CREATEDAT_FIELD_NAME, DataTableApiConstant.DATETIME_FIELD_TYPE, false, null));
            columns.add(addColumn(DataTableApiConstant.UPDATEDAT_FIELD_NAME, DataTableApiConstant.DATETIME_FIELD_TYPE, false, null));
            for (final JsonElement column : columns) {
                parseDatatableColumnObjectForCreate(column.getAsJsonObject(), sqlBuilder, constrainBuilder, dataTableNameAlias,
                        codeMappings, isConstraintApproach);
            }

            // Remove trailing comma and space
            sqlBuilder.delete(sqlBuilder.length() - 2, sqlBuilder.length());

            String fullFkName = FK_PREFIX + fkName;
            if (multiRow) {
                sqlBuilder.append(", PRIMARY KEY (id)");
                if (databaseTypeResolver.isMySQL()) {
                    sqlBuilder.append(", KEY ").append(sqlGenerator.escape(FK_PREFIX + apptableName.substring(2) + ID_POSTFIX))
                            .append(SPACE).append(BRACKET_OPENING).append(sqlGenerator.escape(fkColumnName)).append(BRACKET_CLOSING);
                }
                sqlBuilder.append(CONSTRAINT).append(sqlGenerator.escape(fullFkName)).append(SPACE).append(FOREIGN_KEY)
                        .append(sqlGenerator.escape(fkColumnName)).append(BRACKET_CLOSING).append(SPACE).append(REFERENCES_)
                        .append(sqlGenerator.escape(actualAppTableName)).append(ID1);
            } else {
                sqlBuilder.append(", PRIMARY KEY (").append(sqlGenerator.escape(fkColumnName)).append(BRACKET_CLOSING).append(CONSTRAINT)
                        .append(sqlGenerator.escape(fullFkName)).append(SPACE).append(FOREIGN_KEY).append(sqlGenerator.escape(fkColumnName))
                        .append(BRACKET_CLOSING).append(SPACE).append(REFERENCES_).append(sqlGenerator.escape(actualAppTableName))
                        .append(ID1);
            }

            sqlBuilder.append(constrainBuilder);
            sqlBuilder.append(BRACKET_CLOSING);
            if (databaseTypeResolver.isMySQL()) {
                sqlBuilder.append(" ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;");
            }
            LOG.debug("SQL:: {}", sqlBuilder);

            this.jdbcTemplate.execute(sqlBuilder.toString());

            registerDatatable(datatableName, apptableName, entitySubType);
            registerColumnCodeMapping(codeMappings);
        } catch (final PersistenceException | DataAccessException e) {
            final Throwable realCause = e.getCause();
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("datatable");

            if (realCause.getMessage().toLowerCase().contains("duplicate column name")) {
                baseDataValidator.reset().parameter(NAME).failWithCode("duplicate.column.name");
            } else if ((realCause.getMessage().contains("Table") || realCause.getMessage().contains("relation"))
                    && realCause.getMessage().contains("already exists")) {
                baseDataValidator.reset().parameter(DATATABLE_NAME).value(datatableName).failWithCode("datatable.already.exists");
            } else if (realCause.getMessage().contains("Column") && realCause.getMessage().contains("big")) {
                baseDataValidator.reset().parameter("column").failWithCode("length.too.big");
            } else if (realCause.getMessage().contains("Row") && realCause.getMessage().contains("large")) {
                baseDataValidator.reset().parameter("row").failWithCode("size.too.large");
            }

            throwExceptionIfValidationWarningsExist(dataValidationErrors);
        }

        return new CommandProcessingResultBuilder().withCommandId(command.commandId()).withResourceIdAsString(datatableName).build();
    }

    private long addMultirowRecord(String sql) throws SQLException {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        int insertsCount = this.jdbcTemplate.update(c -> c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS), keyHolder);
        if (insertsCount == 1) {
            Number assignedKey;
            if (keyHolder.getKeys().size() > 1) {
                assignedKey = (Long) keyHolder.getKeys().get(ID);
            } else {
                assignedKey = keyHolder.getKey();
            }
            if (assignedKey == null) {
                throw new SQLException("Row id getting error.");
            }
            return assignedKey.longValue();
        }
        throw new SQLException("Expected one inserted row.");
    }

    private void parseDatatableColumnForUpdate(final JsonObject column,
            final Map<String, ResultsetColumnHeaderData> mapColumnNameDefinition, StringBuilder sqlBuilder, final String datatableName,
            final StringBuilder constrainBuilder, final Map<String, Long> codeMappings, final List<String> removeMappings,
            final boolean isConstraintApproach) {

        String name = column.has(NAME) ? column.get(NAME).getAsString() : null;
        final String lengthStr = column.has(LENGTH) ? column.get(LENGTH).getAsString() : null;
        Integer length = StringUtils.isNotBlank(lengthStr) ? Integer.parseInt(lengthStr) : null;
        String newName = column.has(NEW_NAME) ? column.get(NEW_NAME).getAsString() : name;
        final boolean mandatory = column.has(MANDATORY) && column.get(MANDATORY).getAsBoolean();
        final String after = column.has(AFTER) ? column.get(AFTER).getAsString() : null;
        final String code = column.has(CODE) ? column.get(CODE).getAsString() : null;
        final String newCode = column.has(NEW_CODE) ? column.get(NEW_CODE).getAsString() : null;
        final String dataTableNameAlias = datatableName.toLowerCase().replaceAll(SPACE_REGEXP, UNDERSCORE);
        if (isConstraintApproach) {
            if (StringUtils.isBlank(newName)) {
                newName = name;
            }
            String fkName = FK_PREFIX + dataTableNameAlias + UNDERSCORE + name;
            String newFkName = FK_PREFIX + dataTableNameAlias + UNDERSCORE + newName;
            if (!StringUtils.equalsIgnoreCase(code, newCode) || !StringUtils.equalsIgnoreCase(name, newName)) {
                if (StringUtils.equalsIgnoreCase(code, newCode)) {
                    final int codeId = getCodeIdForColumn(dataTableNameAlias, name);
                    if (codeId > 0) {
                        removeMappings.add(dataTableNameAlias + UNDERSCORE + name);
                        constrainBuilder.append(DROP_FOREIGN_KEY_CONSTRAINT).append(sqlGenerator.escape(fkName)).append(SPACE);
                        codeMappings.put(dataTableNameAlias + UNDERSCORE + newName, (long) codeId);
                        constrainBuilder.append(ADD_CONSTRAINT_SQL).append(sqlGenerator.escape(newFkName)).append(SPACE).append(FOREIGN_KEY)
                                .append(sqlGenerator.escape(newName)).append(BRACKET_CLOSING).append(SPACE).append(REFERENCES_)
                                .append(sqlGenerator.escape(CODE_VALUES_TABLE)).append(ID1);
                    }

                } else {
                    if (code != null) {
                        removeMappings.add(dataTableNameAlias + UNDERSCORE + name);
                        if (newCode == null || !StringUtils.equalsIgnoreCase(name, newName)) {
                            constrainBuilder.append(DROP_FOREIGN_KEY_CONSTRAINT).append(sqlGenerator.escape(fkName)).append(SPACE);
                        }
                    }
                    if (newCode != null) {
                        codeMappings.put(dataTableNameAlias + UNDERSCORE + newName,
                                this.codeReadPlatformService.retriveCode(newCode).getCodeId());
                        if (code == null || !StringUtils.equalsIgnoreCase(name, newName)) {
                            constrainBuilder.append(ADD_CONSTRAINT_SQL).append(sqlGenerator.escape(newFkName)).append(SPACE)
                                    .append(FOREIGN_KEY).append(sqlGenerator.escape(newName)).append(BRACKET_CLOSING).append(SPACE)
                                    .append(REFERENCES_).append(sqlGenerator.escape(CODE_VALUES_TABLE)).append(ID1);
                        }
                    }
                }
            }
        } else {
            if (StringUtils.isNotBlank(code)) {
                name = datatableColumnNameToCodeValueName(name, code);
                if (StringUtils.isNotBlank(newCode)) {
                    newName = datatableColumnNameToCodeValueName(newName, newCode);
                } else {
                    newName = datatableColumnNameToCodeValueName(newName, code);
                }
            }
        }
        if (!mapColumnNameDefinition.containsKey(name)) {
            throw new PlatformDataIntegrityException("error.msg.datatable.column.missing.update.parse",
                    "Column " + name + " does not exist.", name);
        }
        final String type = mapColumnNameDefinition.get(name).getColumnType();
        if (length == null && type.equalsIgnoreCase(VARCHAR)) {
            length = mapColumnNameDefinition.get(name).getColumnLength().intValue();
        }

        sqlBuilder.append(CHANGE_SQL).append(sqlGenerator.escape(name)).append(SPACE).append(sqlGenerator.escape(newName)).append(SPACE)
                .append(type);
        if (length != null && length > 0) {
            if (type.equalsIgnoreCase(DECIMAL)) {
                sqlBuilder.append(DECIMAL_LENGTH);
            } else if (type.equalsIgnoreCase(VARCHAR)) {
                sqlBuilder.append(BRACKET_OPENING).append(length).append(BRACKET_CLOSING);
            }
        }

        if (mandatory) {
            sqlBuilder.append(NOT_NULL);
        } else {
            sqlBuilder.append(DEFAULT_NULL);
        }

        if (after != null) {
            sqlBuilder.append(AFTER_SQL).append(sqlGenerator.escape(after));
        }
    }

    private int getCodeIdForColumn(final String dataTableNameAlias, final String name) {
        final StringBuilder checkColumnCodeMapping = new StringBuilder();
        checkColumnCodeMapping.append(SELECT_CODEID_FROM_MAPPINGS_SQL).append(dataTableNameAlias).append(UNDERSCORE).append(name)
                .append(SINGLE_QUOTE);
        Integer codeId = 0;
        try {
            codeId = this.jdbcTemplate.queryForObject(checkColumnCodeMapping.toString(), Integer.class);
        } catch (final EmptyResultDataAccessException e) {
            LOG.warn("Error occurred.", e);
        }
        return ObjectUtils.defaultIfNull(codeId, 0);
    }

    private void parseDatatableColumnForAdd(final JsonObject column, StringBuilder sqlBuilder, final String dataTableNameAlias,
            final StringBuilder constrainBuilder, final Map<String, Long> codeMappings, final boolean isConstraintApproach) {

        String name = column.has(NAME) ? column.get(NAME).getAsString() : null;
        final String type = column.has(TYPE) ? column.get(TYPE).getAsString().toLowerCase() : null;
        final Integer length = column.has(LENGTH) ? column.get(LENGTH).getAsInt() : null;
        final boolean mandatory = column.has(MANDATORY) && column.get(MANDATORY).getAsBoolean();
        final String after = column.has(AFTER) ? column.get(AFTER).getAsString() : null;
        final String code = column.has(CODE) ? column.get(CODE).getAsString() : null;

        if (StringUtils.isNotBlank(code)) {
            if (isConstraintApproach) {
                String fkName = FK_PREFIX + dataTableNameAlias + UNDERSCORE + name;
                codeMappings.put(dataTableNameAlias + UNDERSCORE + name, this.codeReadPlatformService.retriveCode(code).getCodeId());
                constrainBuilder.append(ADD_CONSTRAINT_SQL).append(sqlGenerator.escape(fkName)).append(SPACE).append(FOREIGN_KEY)
                        .append(sqlGenerator.escape(name)).append(BRACKET_CLOSING).append(SPACE).append(REFERENCES_)
                        .append(sqlGenerator.escape(CODE_VALUES_TABLE)).append(ID1);
            } else {
                name = datatableColumnNameToCodeValueName(name, code);
            }
        }

        final String dataType;
        if (databaseTypeResolver.isMySQL()) {
            dataType = apiTypeToMySQL.get(type);
        } else if (databaseTypeResolver.isPostgreSQL()) {
            dataType = apiTypeToPostgreSQL.get(type);
        } else {
            throw new IllegalStateException("Current database is not supported");
        }
        sqlBuilder.append(COLON_ADD).append(sqlGenerator.escape(name)).append(SPACE).append(dataType);

        if (type != null) {
            if (type.equalsIgnoreCase(ReadWriteNonCoreDataServiceImpl.STRING) && length != null) {
                sqlBuilder.append(BRACKET_OPENING).append(length).append(BRACKET_CLOSING);
            } else if (type.equalsIgnoreCase(DECIMAL)) {
                sqlBuilder.append(DECIMAL_LENGTH);
            } else if (type.equalsIgnoreCase(DROPDOWN)) {
                sqlBuilder.append(DROPDOWN_LENGHT);
            }
        }

        if (mandatory) {
            sqlBuilder.append(NOT_NULL);
        } else {
            sqlBuilder.append(DEFAULT_NULL);
        }

        if (after != null) {
            sqlBuilder.append(AFTER_SQL).append(sqlGenerator.escape(after));
        }
    }

    private void parseDatatableColumnForDrop(final JsonObject column, final String datatableName, final StringBuilder constrainBuilder,
            final List<String> codeMappings) {
        final String datatableAlias = datatableName.toLowerCase().replaceAll("\\s", "_");
        final String name = column.has("name") ? column.get("name").getAsString() : null;
        final StringBuilder findFKSql = new StringBuilder();
        findFKSql.append("SELECT count(*)").append("FROM information_schema.TABLE_CONSTRAINTS i")
                .append(" WHERE i.CONSTRAINT_TYPE = 'FOREIGN KEY'").append(" AND i.TABLE_SCHEMA = DATABASE()")
                .append(" AND i.TABLE_NAME = '").append(datatableName).append("' AND i.CONSTRAINT_NAME = 'fk_").append(datatableAlias)
                .append("_").append(name).append("' ");
        final int count = this.jdbcTemplate.queryForObject(findFKSql.toString(), Integer.class);
        if (count > 0) {
            String fkName = "fk_" + datatableAlias + "_" + name;
            codeMappings.add(datatableAlias + "_" + name);
            constrainBuilder.append(", DROP FOREIGN KEY ").append(sqlGenerator.escape(fkName)).append(" ");
        }
    }

    private void registerColumnCodeMapping(final Map<String, Long> codeMappings) {
        if (codeMappings != null && !codeMappings.isEmpty()) {
            final String[] addSqlList = new String[codeMappings.size()];
            int i = 0;
            for (final Map.Entry<String, Long> mapEntry : codeMappings.entrySet()) {
                addSqlList[i++] = "insert into x_table_column_code_mappings (column_alias_name, code_id) values ('" + mapEntry.getKey()
                        + "'," + mapEntry.getValue() + ");";
            }

            this.jdbcTemplate.batchUpdate(addSqlList);
        }
    }

    private void deleteColumnCodeMapping(final List<String> columnNames) {
        if (columnNames != null && !columnNames.isEmpty()) {
            final String[] deleteSqlList = new String[columnNames.size()];
            int i = 0;
            for (final String columnName : columnNames) {
                deleteSqlList[i++] = "DELETE FROM x_table_column_code_mappings WHERE  column_alias_name='" + columnName + "';";
            }

            this.jdbcTemplate.batchUpdate(deleteSqlList);
        }

    }

    /**
     * Update data table, set column value to empty string where current value is NULL. Run update SQL only if the
     * "mandatory" property is set to true
     *
     * @param datatableName
     *            Name of data table
     * @param column
     *            JSON encoded array of column properties
     * @see <a href="https://mifosforge.jira.com/browse/MIFOSX-1145">MIFOSX-1145</a>
     **/
    private void removeNullValuesFromStringColumn(final String datatableName, final JsonObject column,
            final Map<String, ResultsetColumnHeaderData> mapColumnNameDefinition) {
        final boolean mandatory = column.has(MANDATORY) && column.get(MANDATORY).getAsBoolean();
        final String name = column.has(NAME) ? column.get(NAME).getAsString() : EMPTY_STRING;
        final String type = mapColumnNameDefinition.containsKey(name) ? mapColumnNameDefinition.get(name).getColumnType() : EMPTY_STRING;

        if (StringUtils.isNotEmpty(type)) {
            if (mandatory && stringDataTypes.contains(type.toLowerCase())) {
                String sqlBuilder = "UPDATE " + sqlGenerator.escape(datatableName) + " SET " + sqlGenerator.escape(name) + " = '' WHERE "
                        + sqlGenerator.escape(name) + " IS NULL";

                this.jdbcTemplate.update(sqlBuilder);
            }
        }
    }

    @Transactional
    @Override
    public void updateDatatable(final String datatableName, final JsonCommand command) {

        try {
            this.context.authenticatedUser();
            this.fromApiJsonDeserializer.validateForUpdate(command.json());

            final JsonElement element = this.fromJsonHelper.parse(command.json());
            final JsonArray changeColumns = this.fromJsonHelper.extractJsonArrayNamed("changeColumns", element);
            final JsonArray addColumns = this.fromJsonHelper.extractJsonArrayNamed("addColumns", element);
            final JsonArray dropColumns = this.fromJsonHelper.extractJsonArrayNamed("dropColumns", element);
            final String apptableName = this.fromJsonHelper.extractStringNamed(APPTABLE_NAME, element);
            final String entitySubType = this.fromJsonHelper.extractStringNamed(ENTITY_SUB_TYPE, element);

            validateDatatableName(datatableName);
            int rowCount = getRowCount(datatableName);
            final List<ResultsetColumnHeaderData> columnHeaderData = this.genericDataService.fillResultsetColumnHeaders(datatableName);
            final Map<String, ResultsetColumnHeaderData> mapColumnNameDefinition = new HashMap<>();
            for (final ResultsetColumnHeaderData columnHeader : columnHeaderData) {
                mapColumnNameDefinition.put(columnHeader.getColumnName(), columnHeader);
            }

            final boolean isConstraintApproach = this.configurationDomainService.isConstraintApproachEnabledForDatatables();

            if (!StringUtils.isBlank(entitySubType)) {
                jdbcTemplate.update("update x_registered_table SET entity_subtype=? WHERE registered_table_name = ?", // NOSONAR
                        entitySubType, datatableName);
            }

            if (!StringUtils.isBlank(apptableName)) {
                validateAppTable(apptableName);

                final String oldApptableName = queryForApplicationTableName(datatableName);
                if (!StringUtils.equals(oldApptableName, apptableName)) {
                    final String oldFKName = oldApptableName.substring(2) + ID_POSTFIX;
                    final String newFKName = apptableName.substring(2) + ID_POSTFIX;
                    final String actualAppTableName = mapToActualAppTable(apptableName);
                    final String oldConstraintName = datatableName.toLowerCase().replaceAll(SPACE_REGEXP, UNDERSCORE) + UNDERSCORE
                            + oldFKName;
                    final String newConstraintName = datatableName.toLowerCase().replaceAll(SPACE_REGEXP, UNDERSCORE) + UNDERSCORE
                            + newFKName;
                    StringBuilder sqlBuilder = new StringBuilder();

                    String fullOldFk = FK_PREFIX + oldFKName;
                    String fullOldConstraint = FK_PREFIX + oldConstraintName;
                    String fullNewFk = FK_PREFIX + newFKName;
                    String fullNewConstraint = FK_PREFIX + newConstraintName;
                    if (mapColumnNameDefinition.containsKey(ID)) {
                        sqlBuilder.append("ALTER TABLE ").append(sqlGenerator.escape(datatableName)).append(SPACE).append("DROP KEY ")
                                .append(sqlGenerator.escape(fullOldFk)).append(",").append("DROP FOREIGN KEY ")
                                .append(sqlGenerator.escape(fullOldConstraint)).append(",").append("CHANGE COLUMN ")
                                .append(sqlGenerator.escape(oldFKName)).append(SPACE).append(sqlGenerator.escape(newFKName))
                                .append(" BIGINT NOT NULL,").append("ADD KEY ").append(sqlGenerator.escape(fullNewFk)).append(SPACE)
                                .append(BRACKET_OPENING).append(sqlGenerator.escape(newFKName)).append("),").append("ADD CONSTRAINT ")
                                .append(sqlGenerator.escape(fullNewConstraint)).append(SPACE).append(FOREIGN_KEY)
                                .append(sqlGenerator.escape(newFKName)).append(BRACKET_CLOSING).append(SPACE).append(REFERENCES_)
                                .append(sqlGenerator.escape(actualAppTableName)).append(ID1);
                    } else {
                        sqlBuilder.append("ALTER TABLE ").append(sqlGenerator.escape(datatableName)).append(SPACE)
                                .append("DROP FOREIGN KEY ").append(sqlGenerator.escape(fullOldConstraint)).append(",")
                                .append("CHANGE COLUMN ").append(sqlGenerator.escape(oldFKName)).append(SPACE)
                                .append(sqlGenerator.escape(newFKName)).append(" BIGINT NOT NULL,").append("ADD CONSTRAINT ")
                                .append(sqlGenerator.escape(fullNewConstraint)).append(SPACE).append(FOREIGN_KEY)
                                .append(sqlGenerator.escape(newFKName)).append(BRACKET_CLOSING).append(SPACE).append(REFERENCES_)
                                .append(sqlGenerator.escape(actualAppTableName)).append(ID1);
                    }

                    this.jdbcTemplate.execute(sqlBuilder.toString());

                    deregisterDatatable(datatableName);
                    registerDatatable(datatableName, apptableName, entitySubType);
                }
            }

            if (changeColumns == null && addColumns == null && dropColumns == null) {
                return;
            }

            if (dropColumns != null) {
                if (rowCount > 0) {
                    throw new GeneralPlatformDomainRuleException("error.msg.non.empty.datatable.column.cannot.be.deleted",
                            "Non-empty datatable columns can not be deleted.");
                }
                StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE " + sqlGenerator.escape(datatableName));
                final StringBuilder constrainBuilder = new StringBuilder();
                final List<String> codeMappings = new ArrayList<>();
                for (final JsonElement column : dropColumns) {
                    parseDatatableColumnForDrop(column.getAsJsonObject(), datatableName, constrainBuilder, codeMappings);
                }

                // Remove the first comma, right after ALTER TABLE datatable
                final int indexOfFirstComma = sqlBuilder.indexOf(",");
                if (indexOfFirstComma != -1) {
                    sqlBuilder.deleteCharAt(indexOfFirstComma);
                }
                sqlBuilder.append(constrainBuilder);
                this.jdbcTemplate.execute(sqlBuilder.toString());
                deleteColumnCodeMapping(codeMappings);
            }
            if (addColumns != null) {

                StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE " + sqlGenerator.escape(datatableName));
                final StringBuilder constrainBuilder = new StringBuilder();
                final Map<String, Long> codeMappings = new HashMap<>();
                for (final JsonElement column : addColumns) {
                    JsonObject columnAsJson = column.getAsJsonObject();
                    if (rowCount > 0 && columnAsJson.has(MANDATORY) && columnAsJson.get(MANDATORY).getAsBoolean()) {
                        throw new GeneralPlatformDomainRuleException("error.msg.non.empty.datatable.mandatory.column.cannot.be.added",
                                "Non empty datatable mandatory columns can not be added.");
                    }
                    parseDatatableColumnForAdd(columnAsJson, sqlBuilder, datatableName.toLowerCase().replaceAll(SPACE_REGEXP, UNDERSCORE),
                            constrainBuilder, codeMappings, isConstraintApproach);
                }

                // Remove the first comma, right after ALTER TABLE datatable
                final int indexOfFirstComma = sqlBuilder.indexOf(",");
                if (indexOfFirstComma != -1) {
                    sqlBuilder.deleteCharAt(indexOfFirstComma);
                }
                sqlBuilder.append(constrainBuilder);
                this.jdbcTemplate.execute(sqlBuilder.toString());
                registerColumnCodeMapping(codeMappings);
            }
            if (changeColumns != null) {

                StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE " + sqlGenerator.escape(datatableName));
                final StringBuilder constrainBuilder = new StringBuilder();
                final Map<String, Long> codeMappings = new HashMap<>();
                final List<String> removeMappings = new ArrayList<>();
                for (final JsonElement column : changeColumns) {
                    // remove NULL values from column where mandatory is true
                    removeNullValuesFromStringColumn(datatableName, column.getAsJsonObject(), mapColumnNameDefinition);

                    parseDatatableColumnForUpdate(column.getAsJsonObject(), mapColumnNameDefinition, sqlBuilder, datatableName,
                            constrainBuilder, codeMappings, removeMappings, isConstraintApproach);
                }

                // Remove the first comma, right after ALTER TABLE datatable
                final int indexOfFirstComma = sqlBuilder.indexOf(",");
                if (indexOfFirstComma != -1) {
                    sqlBuilder.deleteCharAt(indexOfFirstComma);
                }
                sqlBuilder.append(constrainBuilder);
                try {
                    this.jdbcTemplate.execute(sqlBuilder.toString());
                    deleteColumnCodeMapping(removeMappings);
                    registerColumnCodeMapping(codeMappings);
                } catch (final Exception e) {
                    if (e.getMessage().contains("Error on rename")) {
                        throw new PlatformServiceUnavailableException("error.msg.datatable.column.update.not.allowed",
                                "One of the column name modification not allowed", e);
                    }
                    // handle all other exceptions in here

                    // check if exception message contains the
                    // "invalid use of null value" SQL exception message
                    // throw a 503 HTTP error -
                    // PlatformServiceUnavailableException
                    if (e.getMessage().toLowerCase().contains("invalid use of null value")) {
                        throw new PlatformServiceUnavailableException("error.msg.datatable.column.update.not.allowed",
                                "One of the data table columns contains null values", e);
                    }
                }
            }
        } catch (final JpaSystemException | DataIntegrityViolationException e) {
            final Throwable realCause = e.getCause();
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("datatable");

            if (realCause.getMessage().toLowerCase().contains("unknown column")) {
                baseDataValidator.reset().parameter(NAME).failWithCode("does.not.exist");
            } else if (realCause.getMessage().toLowerCase().contains("can't drop")) {
                baseDataValidator.reset().parameter(NAME).failWithCode("does.not.exist");
            } else if (realCause.getMessage().toLowerCase().contains("duplicate column")) {
                baseDataValidator.reset().parameter(NAME).failWithCode("column.already.exists");
            }

            throwExceptionIfValidationWarningsExist(dataValidationErrors);
        } catch (final PersistenceException ee) {
            Throwable realCause = ExceptionUtils.getRootCause(ee.getCause());
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("datatable");
            if (realCause.getMessage().toLowerCase().contains("duplicate column name")) {
                baseDataValidator.reset().parameter(NAME).failWithCode("duplicate.column.name");
            } else if ((realCause.getMessage().contains("Table") || realCause.getMessage().contains("relation"))
                    && realCause.getMessage().contains("already exists")) {
                baseDataValidator.reset().parameter(DATATABLE_NAME).value(datatableName).failWithCode("datatable.already.exists");
            } else if (realCause.getMessage().contains("Column") && realCause.getMessage().contains("big")) {
                baseDataValidator.reset().parameter("column").failWithCode("length.too.big");
            } else if (realCause.getMessage().contains("Row") && realCause.getMessage().contains("large")) {
                baseDataValidator.reset().parameter("row").failWithCode("size.too.large");
            }

            throwExceptionIfValidationWarningsExist(dataValidationErrors);
        }
    }

    @Transactional
    @Override
    public void deleteDatatable(final String datatableName) {

        try {
            this.context.authenticatedUser();
            if (!isRegisteredDataTable(datatableName)) {
                throw new DatatableNotFoundException(datatableName);
            }
            validateDatatableName(datatableName);
            assertDataTableEmpty(datatableName);
            deregisterDatatable(datatableName);
            String[] sqlArray;
            if (this.configurationDomainService.isConstraintApproachEnabledForDatatables()) {
                final String deleteColumnCodeSql = "delete from x_table_column_code_mappings where column_alias_name like'"
                        + datatableName.toLowerCase().replaceAll(SPACE_REGEXP, UNDERSCORE) + "_" + PERCENTAGE_SIGN + "'";
                sqlArray = new String[2];
                sqlArray[1] = deleteColumnCodeSql;
            } else {
                sqlArray = new String[1];
            }
            final String sql = "DROP TABLE " + sqlGenerator.escape(datatableName);
            sqlArray[0] = sql;
            this.jdbcTemplate.batchUpdate(sqlArray);
        } catch (final JpaSystemException | DataIntegrityViolationException e) {
            final Throwable realCause = e.getCause();
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final DataValidatorBuilder baseDataValidator = new DataValidatorBuilder(dataValidationErrors).resource("datatable");
            if (realCause.getMessage().contains("Unknown table")) {
                baseDataValidator.reset().parameter(DATATABLE_NAME).failWithCode("does.not.exist");
            }

            throwExceptionIfValidationWarningsExist(dataValidationErrors);
        }
    }

    private void assertDataTableEmpty(final String datatableName) {
        final int rowCount = getRowCount(datatableName);
        if (rowCount != 0) {
            throw new GeneralPlatformDomainRuleException("error.msg.non.empty.datatable.cannot.be.deleted",
                    "Non-empty datatable cannot be deleted.");
        }
    }

    private int getRowCount(final String datatableName) {
        final String sql = "select count(*) from " + sqlGenerator.escape(datatableName);
        return this.jdbcTemplate.queryForObject(sql, Integer.class); // NOSONAR
    }

    @Transactional
    @Override
    public CommandProcessingResult updateDatatableEntryOneToOne(final String dataTableName, final Long appTableId,
            final JsonCommand command) {

        return updateDatatableEntry(dataTableName, appTableId, null, command);
    }

    @Transactional
    @Override
    public CommandProcessingResult updateDatatableEntryOneToMany(final String dataTableName, final Long appTableId, final Long datatableId,
            final JsonCommand command) {

        return updateDatatableEntry(dataTableName, appTableId, datatableId, command);
    }

    private CommandProcessingResult updateDatatableEntry(final String dataTableName, final Long appTableId, final Long datatableId,
            final JsonCommand command) {

        final String appTable = queryForApplicationTableName(dataTableName);
        final CommandProcessingResult commandProcessingResult = checkMainResourceExistsWithinScope(appTable, appTableId);

        final GenericResultsetData grs = retrieveDataTableGenericResultSetForUpdate(appTable, dataTableName, appTableId, datatableId);

        if (grs.hasNoEntries()) {
            throw new DatatableNotFoundException(dataTableName, appTableId);
        }

        if (grs.hasMoreThanOneEntry()) {
            throw new PlatformDataIntegrityException("error.msg.attempting.multiple.update",
                    "Application table: " + dataTableName + " Foreign key id: " + appTableId);
        }

        final Type typeOfMap = new TypeToken<Map<String, String>>() {

        }.getType();
        final Map<String, String> dataParams = this.fromJsonHelper.extractDataMap(typeOfMap, command.json());

        String pkName = ID; // 1:M datatable
        if (datatableId == null) {
            pkName = getFKField(appTable);
        } // 1:1 datatable

        final Map<String, Object> changes = getAffectedAndChangedColumns(grs, dataParams, pkName);

        if (!changes.isEmpty()) {
            Long pkValue = appTableId;
            if (datatableId != null) {
                pkValue = datatableId;
            }
            final String sql = getUpdateSql(grs.getColumnHeaders(), dataTableName, pkName, pkValue, changes);
            LOG.debug(UPDATE_SQL, sql);
            if (StringUtils.isNotBlank(sql)) {
                this.jdbcTemplate.update(sql);
            } else {
                LOG.debug(NO_CHANGES);
            }
        }

        return new CommandProcessingResultBuilder() //
                .withOfficeId(commandProcessingResult.getOfficeId()) //
                .withGroupId(commandProcessingResult.getGroupId()) //
                .withClientId(commandProcessingResult.getClientId()) //
                .withSavingsId(commandProcessingResult.getSavingsId()) //
                .withLoanId(commandProcessingResult.getLoanId()) //
                .with(changes) //
                .build();
    }

    @Transactional
    @Override
    public CommandProcessingResult deleteDatatableEntries(final String dataTableName, final Long appTableId) {

        validateDatatableName(dataTableName);
        if (isDatatableAttachedToEntityDatatableCheck(dataTableName)) {
            throw new DatatableEntryRequiredException(dataTableName, appTableId);
        }
        final String appTable = queryForApplicationTableName(dataTableName);
        final CommandProcessingResult commandProcessingResult = checkMainResourceExistsWithinScope(appTable, appTableId);
        final String deleteOneToOneEntrySql = getDeleteEntriesSql(dataTableName, getFKField(appTable), appTableId);

        final int rowsDeleted = this.jdbcTemplate.update(deleteOneToOneEntrySql);
        if (rowsDeleted < 1) {
            throw new DatatableNotFoundException(dataTableName, appTableId);
        }

        return commandProcessingResult;
    }

    @Transactional
    @Override
    public CommandProcessingResult deleteDatatableEntry(final String dataTableName, final Long appTableId, final Long datatableId) {
        validateDatatableName(dataTableName);
        if (isDatatableAttachedToEntityDatatableCheck(dataTableName)) {
            throw new DatatableEntryRequiredException(dataTableName, appTableId);
        }
        final String appTable = queryForApplicationTableName(dataTableName);
        final CommandProcessingResult commandProcessingResult = checkMainResourceExistsWithinScope(appTable, appTableId);

        final String sql = getDeleteEntrySql(dataTableName, datatableId);

        this.jdbcTemplate.update(sql);
        return commandProcessingResult;
    }

    @Override
    public GenericResultsetData retrieveDataTableGenericResultSet(final String dataTableName, final Long appTableId, final String order,
            final Long id) {

        final String appTable = queryForApplicationTableName(dataTableName);

        checkMainResourceExistsWithinScope(appTable, appTableId);

        final List<ResultsetColumnHeaderData> columnHeaders = this.genericDataService.fillResultsetColumnHeaders(dataTableName);

        final boolean multiRow = isMultirowDatatable(columnHeaders);

        String whereClause = getFKField(appTable) + EQUALS + appTableId;
        SQLInjectionValidator.validateSQLInput(whereClause);
        String sql = SELECT_FROM + sqlGenerator.escape(dataTableName) + WHERE + whereClause;

        // id only used for reading a specific entry that belongs to appTableId (in a one to many datatable)
        if (multiRow && id != null) {
            sql = sql + AND_ID + id;
        }

        if (StringUtils.isNotBlank(order)) {
            this.columnValidator.validateSqlInjection(sql, order);
            sql = sql + ORDER_BY + order;
        }

        final List<ResultsetRowData> result = fillDatatableResultSetDataRows(sql, columnHeaders);

        return new GenericResultsetData(columnHeaders, result);
    }

    private GenericResultsetData retrieveDataTableGenericResultSetForUpdate(final String appTable, final String dataTableName,
            final Long appTableId, final Long id) {

        final List<ResultsetColumnHeaderData> columnHeaders = this.genericDataService.fillResultsetColumnHeaders(dataTableName);

        final boolean multiRow = isMultirowDatatable(columnHeaders);

        String whereClause = getFKField(appTable) + EQUALS + appTableId;
        SQLInjectionValidator.validateSQLInput(whereClause);
        String sql = SELECT_FROM + sqlGenerator.escape(dataTableName) + WHERE + whereClause;

        // id only used for reading a specific entry that belongs to appTableId (in a one to many datatable)
        if (multiRow && id != null) {
            sql = sql + AND_ID + id;
        }

        final List<ResultsetRowData> result = fillDatatableResultSetDataRows(sql, columnHeaders);

        return new GenericResultsetData(columnHeaders, result);
    }

    private CommandProcessingResult checkMainResourceExistsWithinScope(final String appTable, final Long appTableId) {

        final SqlRowSet rs = dataScopedSQL(appTable, appTableId);

        if (!rs.next()) {
            throw new DatatableNotFoundException(appTable, appTableId);
        }

        final Long officeId = getLongSqlRowSet(rs, OFFICE_ID);
        final Long groupId = getLongSqlRowSet(rs, GROUP_ID);
        final Long clientId = getLongSqlRowSet(rs, CLIENT_ID);
        final Long savingsId = getLongSqlRowSet(rs, SAVINGS_ID);
        final Long LoanId = getLongSqlRowSet(rs, LOAN_ID);
        final Long entityId = getLongSqlRowSet(rs, ENTITY_ID);

        if (rs.next()) {
            throw new DatatableSystemErrorException(SYSTEM_ERROR_MORE_THAN_ONE_ROW_RETURNED_FROM_DATA_SCOPING_QUERY);
        }

        return new CommandProcessingResultBuilder() //
                .withOfficeId(officeId) //
                .withGroupId(groupId) //
                .withClientId(clientId) //
                .withSavingsId(savingsId) //
                .withLoanId(LoanId).withEntityId(entityId)//
                .build();
    }

    private Long getLongSqlRowSet(final SqlRowSet rs, final String column) {
        Long val = rs.getLong(column);
        if (val == 0) {
            val = null;
        }
        return val;
    }

    private SqlRowSet dataScopedSQL(final String appTable, final Long appTableId) {
        /*
         * unfortunately have to, one way or another, be able to restrict data to the users office hierarchy. Here, a
         * few key tables are done. But if additional fields are needed on other tables the same pattern applies
         */

        final AppUser currentUser = this.context.authenticatedUser();
        String scopedSQL = null;
        SqlRowSet sqlRowSet = null;
        /*
         * m_loan and m_savings_account are connected to an m_office thru either an m_client or an m_group If both it
         * means it relates to an m_client that is in a group (still an m_client account)
         */
        if (appTable.equalsIgnoreCase(M_LOAN)) {
            scopedSQL = LOAN_DATA_SCOPED_SQL;
            sqlRowSet = this.jdbcTemplate.queryForRowSet(scopedSQL, currentUser.getOffice().getHierarchy() + PERCENTAGE_SIGN, appTableId,
                    currentUser.getOffice().getHierarchy() + PERCENTAGE_SIGN, appTableId);
        } else if (appTable.equalsIgnoreCase(M_SAVINGS_ACCOUNT)) {
            scopedSQL = SAVINGS_DATA_SCOPED_SQL;
            sqlRowSet = this.jdbcTemplate.queryForRowSet(scopedSQL, currentUser.getOffice().getHierarchy() + PERCENTAGE_SIGN, appTableId,
                    currentUser.getOffice().getHierarchy() + PERCENTAGE_SIGN, appTableId);
        } else if (appTable.equalsIgnoreCase(M_CLIENT)) {
            scopedSQL = CLIENT_DATA_SCOPED_SQL;
            sqlRowSet = this.jdbcTemplate.queryForRowSet(scopedSQL, currentUser.getOffice().getHierarchy() + PERCENTAGE_SIGN, appTableId);
        } else if (appTable.equalsIgnoreCase(M_GROUP) || appTable.equalsIgnoreCase(M_CENTER)) {
            scopedSQL = GROUP_DATA_SCOPED_SQL;
            sqlRowSet = this.jdbcTemplate.queryForRowSet(scopedSQL, currentUser.getOffice().getHierarchy() + PERCENTAGE_SIGN, appTableId);
        } else if (appTable.equalsIgnoreCase(M_OFFICE)) {
            scopedSQL = OFFICE_DATA_SCOPED_SQL;
            this.jdbcTemplate.queryForRowSet(scopedSQL, currentUser.getOffice().getHierarchy() + PERCENTAGE_SIGN, appTableId);
        } else if (appTable.equalsIgnoreCase(M_PRODUCT_LOAN) || appTable.equalsIgnoreCase(M_SAVINGS_PRODUCT)) {
            scopedSQL = PRODUCT_DATA_SCOPED_SQL;
            sqlRowSet = this.jdbcTemplate.queryForRowSet(scopedSQL, appTable, appTableId);
        } else {
            throw new PlatformDataIntegrityException("error.msg.invalid.dataScopeCriteria",
                    "Application Table: " + appTable + " not catered for in data Scoping");
        }
        LOG.debug(DATA_SCOPED_SQL_FOR_LOGGING, scopedSQL);
        return sqlRowSet;
    }

    private void validateAppTable(final String appTable) {

        if (appTable.equalsIgnoreCase(M_LOAN)) {
            return;
        }
        if (appTable.equalsIgnoreCase(M_SAVINGS_ACCOUNT)) {
            return;
        }
        if (appTable.equalsIgnoreCase(M_CLIENT)) {
            return;
        }
        if (appTable.equalsIgnoreCase(M_GROUP)) {
            return;
        }
        if (appTable.equalsIgnoreCase(M_CENTER)) {
            return;
        }
        if (appTable.equalsIgnoreCase(M_OFFICE)) {
            return;
        }
        if (appTable.equalsIgnoreCase(M_PRODUCT_LOAN)) {
            return;
        }
        if (appTable.equalsIgnoreCase(M_SAVINGS_PRODUCT)) {
            return;
        }

        throw new PlatformDataIntegrityException("error.msg.invalid.application.table", "Invalid Application Table: " + appTable, NAME,
                appTable);
    }

    private String mapToActualAppTable(final String appTable) {
        if (appTable.equalsIgnoreCase(M_CENTER)) {
            return M_GROUP;
        }
        return appTable;
    }

    private List<ResultsetRowData> fillDatatableResultSetDataRows(final String sql, final List<ResultsetColumnHeaderData> columnHeaders) {
        final List<ResultsetRowData> resultsetDataRows = new ArrayList<>();
        final GenericResultsetData genericResultsetData = new GenericResultsetData(columnHeaders, null);

        final SqlRowSet rowSet = jdbcTemplate.queryForRowSet(sql); // NOSONAR
        final SqlRowSetMetaData rsmd = rowSet.getMetaData();

        while (rowSet.next()) {
            final List<Object> columnValues = new ArrayList<>();

            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                final String columnName = rsmd.getColumnName(i + 1);
                final String colType = genericResultsetData.getColTypeOfColumnNamed(columnName);

                if (DATE.equalsIgnoreCase(colType)) {
                    Date tmpDate = (Date) rowSet.getObject(columnName);
                    columnValues.add(tmpDate != null ? tmpDate.toLocalDate() : null);
                } else if (TIMESTAMP_WITHOUT_TIME_ZONE.equalsIgnoreCase(colType) // PostgreSQL
                        || DATETIME.equalsIgnoreCase(colType) || TIMESTAMP.equalsIgnoreCase(colType)) {
                    Object tmpDate = rowSet.getObject(columnName);

                    columnValues
                            .add(tmpDate != null ? tmpDate instanceof Timestamp ? ((Timestamp) tmpDate).toLocalDateTime() : tmpDate : null);
                } else {
                    columnValues.add(rowSet.getObject(columnName));
                }
            }

            resultsetDataRows.add(ResultsetRowData.create(columnValues));
        }

        return resultsetDataRows;
    }

    @Cacheable(value = "applicationTableName", key = "T(org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil).getTenant().getTenantIdentifier().concat(#datatable+'atn')")
    public String queryForApplicationTableName(final String datatable) {
        SQLInjectionValidator.validateSQLInput(datatable);

        String applicationTableName;

        try {
            applicationTableName = jdbcTemplate.queryForObject(
                    SELECT_APPLICATION_TABLE_NAME_FROM_X_REGISTERED_TABLE_WHERE_REGISTERED_TABLE_NAME, String.class, datatable); // NOSONAR
        } catch (EmptyResultDataAccessException e) {
            throw new DatatableNotFoundException(datatable, e);
        }

        return applicationTableName;
    }

    private String getFKField(final String applicationTableName) {
        return applicationTableName.substring(2) + ID_POSTFIX;
    }

    private String getAddSql(final List<ResultsetColumnHeaderData> columnHeaders, final String datatable, final String fkName,
            final Long appTableId, final Map<String, String> queryParams) {

        final Map<String, Object> affectedColumns = getAffectedColumns(columnHeaders, queryParams, fkName);

        String pValueWrite;
        String addSql;
        final String singleQuote = SINGLE_QUOTE;

        StringBuilder insertColumns = new StringBuilder(200);
        StringBuilder selectColumns = new StringBuilder(200);
        String columnName;
        String pValue;
        for (final ResultsetColumnHeaderData pColumnHeader : columnHeaders) {
            final String key = pColumnHeader.getColumnName();
            if (affectedColumns.containsKey(key)) {
                pValue = String.valueOf(affectedColumns.get(key));
                if (StringUtils.isEmpty(pValue) || NULL.equalsIgnoreCase(pValue)) {
                    pValueWrite = NULL;
                } else {
                    if (BIT.equalsIgnoreCase(pColumnHeader.getColumnType())) {
                        if (databaseTypeResolver.isMySQL()) {
                            pValueWrite = BooleanUtils.toString(BooleanUtils.toBooleanObject(pValue), TRUE_AS_NUMBER_STRING,
                                    FALSE_AS_NUMBER_STRING, NULL);
                        } else if (databaseTypeResolver.isPostgreSQL()) {
                            pValueWrite = BooleanUtils.toString(BooleanUtils.toBooleanObject(pValue), B_1, B_0, NULL);
                        } else {
                            throw new IllegalStateException("Current database is not supported");
                        }

                    } else {
                        pValueWrite = singleQuote + this.genericDataService.replace(pValue, singleQuote, singleQuote + singleQuote)
                                + singleQuote;
                    }

                }
                columnName = sqlGenerator.escape(key);
                insertColumns.append(COLON_SPACE).append(columnName);
                selectColumns.append(COLON_SPACE).append(pValueWrite).append(AS).append(columnName);
            } else {
                if (key.equalsIgnoreCase(DataTableApiConstant.CREATEDAT_FIELD_NAME)
                        || key.equalsIgnoreCase(DataTableApiConstant.UPDATEDAT_FIELD_NAME)) {
                    columnName = sqlGenerator.escape(key);
                    insertColumns.append(COLON_SPACE).append(columnName);
                    selectColumns.append(COLON_SPACE).append(sqlGenerator.currentTenantDateTime()).append(AS).append(columnName);
                }
            }
        }

        addSql = INSERT_INTO + sqlGenerator.escape(datatable) + SPACE + BRACKET_OPENING + sqlGenerator.escape(fkName) + SPACE
                + insertColumns + BRACKET_CLOSING + SPACE + SELECT + appTableId + AS_ID + selectColumns;

        LOG.debug(CURLY_BRACKETS, addSql);

        return addSql;
    }

    /**
     * This method is used special for ppi cases Where the score need to be computed
     *
     * @param columnHeaders
     * @param datatable
     * @param fkName
     * @param appTableId
     * @param queryParams
     * @return
     */
    public String getAddSqlWithScore(final List<ResultsetColumnHeaderData> columnHeaders, final String datatable, final String fkName,
            final Long appTableId, final Map<String, String> queryParams) {

        final Map<String, Object> affectedColumns = getAffectedColumns(columnHeaders, queryParams, fkName);

        String pValueWrite;
        StringBuilder scoresId = new StringBuilder(SPACE);
        final String singleQuote = SINGLE_QUOTE;

        StringBuilder insertColumns = new StringBuilder(500);
        StringBuilder selectColumns = new StringBuilder(500);
        String columnName;
        String pValue;
        for (final String key : affectedColumns.keySet()) {
            pValue = String.valueOf(affectedColumns.get(key));

            if (StringUtils.isEmpty(pValue) || NULL.equalsIgnoreCase(pValue)) {
                pValueWrite = NULL;
            } else {
                pValueWrite = singleQuote + this.genericDataService.replace(pValue, singleQuote, singleQuote + singleQuote) + singleQuote;

                scoresId.append(pValueWrite).append(COLON_SPACE);

            }
            columnName = sqlGenerator.escape(key);
            insertColumns.append(COLON_SPACE).append(columnName);
            selectColumns.append(COLON_SPACE).append(pValueWrite).append(AS).append(columnName);
        }

        scoresId = new StringBuilder(scoresId.toString().replaceAll(SPACE_COLON_DOLLAR, EMPTY_STRING));

        String vaddSql = INSERT_INTO + sqlGenerator.escape(datatable) + SPACE + BRACKET_OPENING + sqlGenerator.escape(fkName) + SPACE
                + insertColumns + ", score )" + SELECT + appTableId + " as id" + selectColumns
                + " , ( SELECT SUM( code_score ) FROM m_code_value WHERE m_code_value.id IN (" + scoresId + " ) ) as score";

        LOG.debug(CURLY_BRACKETS, vaddSql);

        return vaddSql;
    }

    private String getUpdateSql(List<ResultsetColumnHeaderData> columnHeaders, final String datatable, final String keyFieldName,
            final Long keyFieldValue, final Map<String, Object> changedColumns) {

        // just updating fields that have changed since pre-update read - though
        // its possible these values are different from the page the user was
        // looking at and even different from the current db values (if some
        // other update got in quick) - would need a version field for
        // completeness but its okay to take this risk with additional fields
        // data

        if (changedColumns.size() == 0) {
            return null;
        }

        String pValue;
        String pValueWrite;
        final String singleQuote = SINGLE_QUOTE;
        boolean firstColumn = true;
        StringBuilder sql = new StringBuilder(UPDATE + sqlGenerator.escape(datatable) + SPACE);
        for (final ResultsetColumnHeaderData pColumnHeader : columnHeaders) {
            final String key = pColumnHeader.getColumnName();
            if (changedColumns.containsKey(key)) {
                if (firstColumn) {
                    sql.append(SET);
                    firstColumn = false;
                } else {
                    sql.append(COLON_SPACE);
                }

                pValue = String.valueOf(changedColumns.get(key));
                if (StringUtils.isEmpty(pValue) || NULL.equalsIgnoreCase(pValue)) {
                    pValueWrite = NULL;
                } else {
                    if (BIT.equalsIgnoreCase(pColumnHeader.getColumnType())) {
                        if (databaseTypeResolver.isMySQL()) {
                            pValueWrite = BooleanUtils.toString(BooleanUtils.toBooleanObject(pValue), TRUE_AS_NUMBER_STRING,
                                    FALSE_AS_NUMBER_STRING, NULL);
                        } else if (databaseTypeResolver.isPostgreSQL()) {
                            pValueWrite = BooleanUtils.toString(BooleanUtils.toBooleanObject(pValue), B_1, B_0, NULL);
                        } else {
                            throw new IllegalStateException("Current database is not supported");
                        }
                    } else {
                        pValueWrite = singleQuote + this.genericDataService.replace(pValue, singleQuote, singleQuote + singleQuote)
                                + singleQuote;
                    }
                }
                sql.append(sqlGenerator.escape(key)).append(EQUALS).append(pValueWrite);
            } else {
                if (key.equalsIgnoreCase(DataTableApiConstant.UPDATEDAT_FIELD_NAME)) {
                    sql.append(COLON_SPACE).append(sqlGenerator.escape(key)).append(EQUALS).append(sqlGenerator.currentTenantDateTime());
                }
            }
        }

        sql.append(WHERE).append(keyFieldName).append(EQUALS).append(keyFieldValue);

        return sql.toString();
    }

    private Map<String, Object> getAffectedAndChangedColumns(final GenericResultsetData grs, final Map<String, String> queryParams,
            final String fkName) {

        final Map<String, Object> affectedColumns = getAffectedColumns(grs.getColumnHeaders(), queryParams, fkName);
        final Map<String, Object> affectedAndChangedColumns = new HashMap<>();

        for (final String key : affectedColumns.keySet()) {
            final Object columnValue = affectedColumns.get(key);
            if (columnChanged(key, columnValue, grs)) {
                affectedAndChangedColumns.put(key, columnValue);
            }
        }

        return affectedAndChangedColumns;
    }

    private boolean columnChanged(final String key, final Object value, final GenericResultsetData grs) {

        final List<Object> columnValues = grs.getData().get(0).getRow();

        Object columnValue;
        for (int i = 0; i < grs.getColumnHeaders().size(); i++) {

            if (key.equals(grs.getColumnHeaders().get(i).getColumnName())) {
                columnValue = columnValues.get(i);

                return notTheSame(columnValue, value);
            }
        }

        throw new PlatformDataIntegrityException("error.msg.invalid.columnName", "Parameter Column Name: " + key + " not found");
    }

    public Map<String, Object> getAffectedColumns(final List<ResultsetColumnHeaderData> columnHeaders,
            final Map<String, String> queryParams, final String keyFieldName) {

        final String dateFormat = queryParams.get(DATE_FORMAT);
        Locale clientApplicationLocale = null;
        final String localeQueryParam = queryParams.get(LOCALE);
        if (!StringUtils.isBlank(localeQueryParam)) {
            clientApplicationLocale = new Locale(queryParams.get(LOCALE));
        }

        String pValue;
        Object validatedValue;
        String queryParamColumnUnderscored;
        String columnHeaderUnderscored;
        boolean notFound;

        final Map<String, Object> affectedColumns = new HashMap<>();
        final Set<String> keys = queryParams.keySet();
        for (final String key : keys) {
            // ignores id and foreign key fields
            // also ignores locale and dateformat fields that are used for
            // validating numeric and date data
            if (!(key.equalsIgnoreCase(ID) || key.equalsIgnoreCase(keyFieldName) || key.equals(LOCALE) || key.equals(DATE_FORMAT)
                    || key.equals(DataTableApiConstant.CREATEDAT_FIELD_NAME) || key.equals(DataTableApiConstant.UPDATEDAT_FIELD_NAME))) {
                notFound = true;
                // matches incoming fields with and without underscores (spaces
                // and underscores considered the same)
                queryParamColumnUnderscored = this.genericDataService.replace(key, SPACE, UNDERSCORE);
                for (final ResultsetColumnHeaderData columnHeader : columnHeaders) {
                    if (notFound) {
                        columnHeaderUnderscored = this.genericDataService.replace(columnHeader.getColumnName(), SPACE, UNDERSCORE);
                        if (queryParamColumnUnderscored.equalsIgnoreCase(columnHeaderUnderscored)) {
                            pValue = queryParams.get(key);
                            validatedValue = validateColumn(columnHeader, pValue, dateFormat, clientApplicationLocale);
                            affectedColumns.put(columnHeader.getColumnName(), validatedValue);
                            notFound = false;
                        }
                    }

                }
                if (notFound) {
                    throw new PlatformDataIntegrityException("error.msg.column.not.found", "Column: " + key + " Not Found");
                }
            }
        }
        return affectedColumns;
    }

    private Object validateColumn(final ResultsetColumnHeaderData columnHeader, final String pValue, final String dateFormat,
            final Locale clientApplicationLocale) {

        String paramValue = pValue;
        if (columnHeader.isDateDisplayType() || columnHeader.isDateTimeDisplayType() || columnHeader.isIntegerDisplayType()
                || columnHeader.isDecimalDisplayType() || columnHeader.isBooleanDisplayType()) {
            // only trim if string is not empty and is not null.
            // throws a NULL pointer exception if the check below is not applied
            paramValue = StringUtils.isNotEmpty(paramValue) ? paramValue.trim() : paramValue;
        }

        if (StringUtils.isEmpty(paramValue) && columnHeader.isMandatory()) {

            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final ApiParameterError error = ApiParameterError.parameterError("error.msg.column.mandatory", "Mandatory",
                    columnHeader.getColumnName());
            dataValidationErrors.add(error);
            throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                    dataValidationErrors);
        }

        if (StringUtils.isNotEmpty(paramValue)) {
            if (columnHeader.hasColumnValues()) {
                if (columnHeader.isCodeValueDisplayType()) {

                    if (columnHeader.isColumnValueNotAllowed(paramValue)) {
                        final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
                        final ApiParameterError error = ApiParameterError.parameterError("error.msg.invalid.columnValue",
                                "Value not found in Allowed Value list", columnHeader.getColumnName(), paramValue);
                        dataValidationErrors.add(error);
                        throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                                dataValidationErrors);
                    }

                    return paramValue;
                } else if (columnHeader.isCodeLookupDisplayType()) {

                    final Integer codeLookup = Integer.valueOf(paramValue);
                    if (columnHeader.isColumnCodeNotAllowed(codeLookup)) {
                        final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
                        final ApiParameterError error = ApiParameterError.parameterError("error.msg.invalid.columnValue",
                                "Value not found in Allowed Value list", columnHeader.getColumnName(), paramValue);
                        dataValidationErrors.add(error);
                        throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                                dataValidationErrors);
                    }

                    return codeLookup;
                } else {
                    throw new PlatformDataIntegrityException("error.msg.invalid.columnType.", "Code: " + columnHeader.getColumnName()
                            + " - Invalid Type " + columnHeader.getColumnType() + " (neither varchar nor int)");
                }
            }

            if (columnHeader.isDateDisplayType()) {
                return JsonParserHelper.convertFrom(paramValue, columnHeader.getColumnName(), dateFormat, clientApplicationLocale);
            } else if (columnHeader.isDateTimeDisplayType()) {
                return JsonParserHelper.convertDateTimeFrom(paramValue, columnHeader.getColumnName(), dateFormat, clientApplicationLocale);
            } else if (columnHeader.isIntegerDisplayType()) {
                return this.helper.convertToInteger(paramValue, columnHeader.getColumnName(), clientApplicationLocale);
            } else if (columnHeader.isDecimalDisplayType()) {
                return this.helper.convertFrom(paramValue, columnHeader.getColumnName(), clientApplicationLocale);
            } else if (columnHeader.isBooleanDisplayType()) {

                final Boolean tmpBoolean = BooleanUtils.toBooleanObject(paramValue);
                if (tmpBoolean == null) {
                    final ApiParameterError error = ApiParameterError
                            .parameterError(
                                    "validation.msg.invalid.boolean.format", "The parameter " + columnHeader.getColumnName()
                                            + " has value: " + paramValue + " which is invalid boolean value.",
                                    columnHeader.getColumnName(), paramValue);
                    final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
                    dataValidationErrors.add(error);
                    throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                            dataValidationErrors);
                }
                return tmpBoolean;
            } else if (columnHeader.isString()) {
                if (columnHeader.getColumnLength() > 0 && paramValue.length() > columnHeader.getColumnLength()) {
                    final ApiParameterError error = ApiParameterError.parameterError(
                            "validation.msg.datatable.entry.column.exceeds.maxlength",
                            "The column `" + columnHeader.getColumnName() + "` exceeds its defined max-length ",
                            columnHeader.getColumnName(), paramValue);
                    final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
                    dataValidationErrors.add(error);
                    throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                            dataValidationErrors);
                }
            }
        } else {
            paramValue = null;
        }
        return paramValue;
    }

    private String getDeleteEntriesSql(final String datatable, final String FKField, final Long appTableId) {

        return DELETE_FROM + sqlGenerator.escape(datatable) + WHERE + sqlGenerator.escape(FKField) + EQUALS + appTableId;

    }

    private String getDeleteEntrySql(final String datatable, final Long datatableId) {

        return DELETE_FROM + sqlGenerator.escape(datatable) + WHERE + ID + EQUALS + datatableId;

    }

    private boolean isMultirowDatatable(final List<ResultsetColumnHeaderData> columnHeaders) {
        boolean multiRow = false;
        for (ResultsetColumnHeaderData column : columnHeaders) {
            if (column.isNamed(ID)) {
                multiRow = true;
                break;
            }
        }
        return multiRow;
    }

    private boolean notTheSame(final Object currValue, final Object pValue) {
        if (currValue == null && pValue == null) {
            return false;
        } else if (currValue == null || pValue == null) {
            return true;
        }
        // Equals would fail if the scale is not the same
        if (currValue instanceof BigDecimal && pValue instanceof BigDecimal) {
            return !(((BigDecimal) currValue).compareTo((BigDecimal) pValue) == 0);
        } else {
            return !currValue.equals(pValue);
        }
    }

    @Override
    public Long countDatatableEntries(final String datatableName, final Long appTableId, String foreignKeyColumn) {
        return this.jdbcTemplate.queryForObject(EXIST_DATATABLE_ENTRIES_SQL, Long.class, sqlGenerator.escape(foreignKeyColumn),
                sqlGenerator.escape(datatableName), sqlGenerator.escape(foreignKeyColumn), appTableId);
    }

    public boolean isDatatableAttachedToEntityDatatableCheck(final String datatableName) {
        return this.jdbcTemplate.queryForObject(IS_TRHERE_DATATABLE_CHECK_FOR_DATATABLE_SQL, Long.class, datatableName) > 0;
    }

}

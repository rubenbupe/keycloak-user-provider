package org.opensingular.dbuserprovider;

import com.google.auto.service.AutoService;
import lombok.extern.jbosslog.JBossLog;
import org.keycloak.Config;
import org.keycloak.component.ComponentModel;
import org.keycloak.component.ComponentValidationException;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.storage.UserStorageProviderFactory;
import org.opensingular.dbuserprovider.model.QueryConfigurations;
import org.opensingular.dbuserprovider.persistence.DataSourceProvider;
import org.opensingular.dbuserprovider.persistence.RDBMS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JBossLog
@AutoService(UserStorageProviderFactory.class)
public class DBUserStorageProviderFactory implements UserStorageProviderFactory<DBUserStorageProvider> {

    public static final String ENV_VAR_URL = "KEYCLOAK_USER_PROVIDER_URL";
    public static final String ENV_VAR_USERNAME = "KEYCLOAK_USER_PROVIDER_USERNAME";
    public static final String ENV_VAR_PASSWORD = "KEYCLOAK_USER_PROVIDER_PASSWORD";
    public static final String ENV_VAR_RDBMS = "KEYCLOAK_USER_PROVIDER_RDBMS";

    public static final String SQL_COUNT =
            "select count(*) from users";

    public static final String SQL_LIST_ALL =
            "select id, username, email from users";

    public static final String SQL_FIND_BY_ID =
            "select id, username, email from users where id = ? ";

    public static final String SQL_FIND_BY_USERNAME =
            "select id, username, email from users where username = ? ";

    public static final String SQL_FIND_BY_SEARCH_TERM =
            "select id, username, email from users where upper(username) like upper(?)";

    public static final String FIND_HASH_PASS_BY_USERNAME =
            "select hash_pwd from users where username = ? ";

    public static final String SHA_256 = "SHA-256";


    private Map<String, ProviderConfig> providerConfigPerInstance = new HashMap<>();

    @Override
    public void init(Config.Scope config) {
    }

    @Override
    public void close() {
        for (Map.Entry<String, ProviderConfig> pc : providerConfigPerInstance.entrySet()) {
            pc.getValue().dataSourceProvider.close();
        }
    }

    @Override
    public DBUserStorageProvider create(KeycloakSession session, ComponentModel model) {
        ProviderConfig providerConfig = providerConfigPerInstance.computeIfAbsent(model.getId(), s -> configure(model));
        return new DBUserStorageProvider(session, model, providerConfig.dataSourceProvider, providerConfig.queryConfigurations);
    }

    private synchronized ProviderConfig configure(ComponentModel model) {
        log.infov("Creating configuration for model: id={0} name={1}", model.getId(), model.getName());

        ProviderConfig providerConfig = new ProviderConfig();

        String url = System.getenv(ENV_VAR_URL);
        if(url == null)
            throw new RuntimeException("url env var is null");

        String user = System.getenv(ENV_VAR_USERNAME);
        if(user == null)
            throw new RuntimeException("user env var is null");

        String password = System.getenv(ENV_VAR_PASSWORD);
        if(password == null)
            throw new RuntimeException("password env var is null");

        RDBMS rdbms = RDBMS.getByDescription(System.getenv(ENV_VAR_RDBMS));
        if(rdbms == null)
            throw new RuntimeException("rdbms env var is null");

        providerConfig.dataSourceProvider.configure(url, rdbms, user, password, model.getName());
        providerConfig.queryConfigurations = new QueryConfigurations(
                SQL_COUNT,
                SQL_LIST_ALL,
                SQL_FIND_BY_ID,
                SQL_FIND_BY_USERNAME,
                SQL_FIND_BY_SEARCH_TERM,
                FIND_HASH_PASS_BY_USERNAME,
                SHA_256,
                rdbms,
                false,
                false
        );
        return providerConfig;
    }

    @Override
    public void validateConfiguration(KeycloakSession session, RealmModel realm, ComponentModel model) throws ComponentValidationException {
        try {
            ProviderConfig old = providerConfigPerInstance.put(model.getId(), configure(model));
            if (old != null) {
                old.dataSourceProvider.close();
            }
        } catch (Exception e) {
            throw new ComponentValidationException(e.getMessage(), e);
        }
    }

    @Override
    public String getId() {
        return "singular-db-user-provider";
    }

    @Override
    public List<ProviderConfigProperty> getConfigProperties() {
        return ProviderConfigurationBuilder.create().build();
    }

    private static class ProviderConfig {
        private DataSourceProvider dataSourceProvider = new DataSourceProvider();
        private QueryConfigurations queryConfigurations;
    }


}

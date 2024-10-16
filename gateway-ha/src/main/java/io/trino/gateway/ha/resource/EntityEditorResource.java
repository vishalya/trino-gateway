package io.trino.gateway.ha.resource;

import static io.trino.gateway.ha.router.ResourceGroupsManager.ResourceGroupsDetail;
import static io.trino.gateway.ha.router.ResourceGroupsManager.SelectorsDetail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.dropwizard.views.View;
import io.trino.gateway.ha.config.ProxyBackendConfiguration;
import io.trino.gateway.ha.router.GatewayBackendManager;
import io.trino.gateway.ha.router.ResourceGroupsManager;
import io.trino.gateway.ha.router.RoutingManager;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import javax.annotation.security.RolesAllowed;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RolesAllowed({"ADMIN"})
@Path("entity")
public class EntityEditorResource {

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  @Inject
  private GatewayBackendManager gatewayBackendManager;
  @Inject
  private ResourceGroupsManager resourceGroupsManager;

  @Inject
  private RoutingManager routingManager;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public EntityView entityUi(@Context SecurityContext securityContext) {
    return new EntityView("/template/entity-view.ftl", securityContext);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public List<EntityType> getAllEntityTypes() {
    return Arrays.asList(EntityType.values());
  }

  @POST
  public Response updateEntity(@QueryParam("entityType") String entityTypeStr,
                               @QueryParam("useSchema") String database,
                               String jsonPayload) {
    if (Strings.isNullOrEmpty(entityTypeStr)) {
      throw new WebApplicationException("EntryType can not be null");
    }
    EntityType entityType = EntityType.valueOf(entityTypeStr);
    try {
      switch (entityType) {
        case GATEWAY_BACKEND:
          //TODO: make the gateway backend database sensitive
          ProxyBackendConfiguration backend =
              OBJECT_MAPPER.readValue(jsonPayload, ProxyBackendConfiguration.class);
          gatewayBackendManager.updateBackend(backend);
          log.info("Setting up the backend {} with healthy state", backend.getName());
          routingManager.upateBackEndHealth(backend.getName(), true);
          break;
        case RESOURCE_GROUP:
          ResourceGroupsDetail resourceGroupDetails = OBJECT_MAPPER.readValue(jsonPayload,
              ResourceGroupsDetail.class);
          resourceGroupsManager.updateResourceGroup(resourceGroupDetails, database);
          break;
        case SELECTOR:
          SelectorsDetail selectorDetails = OBJECT_MAPPER.readValue(jsonPayload,
              SelectorsDetail.class);
          List<SelectorsDetail> oldSelectorDetails =
              resourceGroupsManager.readSelector(selectorDetails.getResourceGroupId(), database);
          if (oldSelectorDetails.size() >= 1) {
            resourceGroupsManager.updateSelector(oldSelectorDetails.get(0),
                selectorDetails, database);
          } else {
            resourceGroupsManager.createSelector(selectorDetails, database);
          }
          break;
        default:
      }
    } catch (IOException e) {
      log.error(e.getMessage(), e);
      throw new WebApplicationException(e);
    }
    return Response.ok().build();
  }

  @GET
  @Path("/{entityType}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllEntitiesForType(@PathParam("entityType") String entityTypeStr,
                                        @QueryParam("useSchema") String database) {
    EntityType entityType = EntityType.valueOf(entityTypeStr);

    switch (entityType) {
      case GATEWAY_BACKEND:
        return Response.ok(gatewayBackendManager.getAllBackends()).build();
      case RESOURCE_GROUP:
        return Response.ok(resourceGroupsManager.readAllResourceGroups(database)).build();
      case SELECTOR:
        return Response.ok(resourceGroupsManager.readAllSelectors(database)).build();
      default:
    }
    return Response.ok(ImmutableList.of()).build();
  }

  @Data
  public static class EntityView extends View {
    private String displayName;

    protected EntityView(String templateName, SecurityContext securityContext) {
      super(templateName, Charset.defaultCharset());
      setDisplayName(securityContext.getUserPrincipal().getName());
    }
  }
}

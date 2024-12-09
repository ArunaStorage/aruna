# TODO

- [ ] Start custom relation types at idx 20, to reserve internal relation idx's
- [ ] Fix Author identifier


- [ ] Add hooks, create a generic workflow / workflow executions table 
- [ ] Decide if workflow runs are part of the graph ?

- [ ] Check that groups are associated with a realm when creating projects


### Ideas
Separate Provenance graph ?

Workflow A  ---- created by wf -------v 
Input A     ---------input-------> Result A
Input B     ---------input------------^



### Requests needed

- Realm Status
- Quotas


- Notifications & GRP Notifications
- Request access to grp / Request access to realm
- AddUserToGrp / AddGrpToRealm
- FilterByRealm Universe


- Check that proxy / realm / group are in line when creating s3creds
- Check that proxy_impersonation_tokens only work with s3creds
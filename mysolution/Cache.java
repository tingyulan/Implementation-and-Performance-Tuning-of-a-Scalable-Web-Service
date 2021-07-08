import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.rmi.registry.*;
import java.rmi.Naming;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;


public class Cache extends UnicastRemoteObject implements Cloud.DatabaseOps {

    private static Cloud.DatabaseOps cache = null;
    private static Map<String, String> cache_data = new ConcurrentHashMap<String, String>();

    public Cache(ServerLib SL) throws RemoteException {
        super();
        cache = SL.getDB();
    }

    // Ask database for a key if can not find it in the cache_data
    // Cache everything
    @Override
	public String get(String key) throws RemoteException {
        String value;
        if(cache_data.containsKey(key)){
            value = cache_data.get(key);
        }else{
            value = cache.get(key);
            cache_data.put(key, value);
        }
        return value;
    }

    // Directly pass set function to database
	@Override
    public boolean set(String key, String value, String auth) throws RemoteException {
        return cache.set(key, value, auth);
    }

    // Directly pass transaction function to database
	@Override
    public boolean transaction(String item, float price, int qty) throws RemoteException {
        return cache.transaction(item, price, qty);
    }

}
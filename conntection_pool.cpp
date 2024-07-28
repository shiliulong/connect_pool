#include <iostream>
#include <sqlite3.h>
#include <string>
#include <vector>
#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <chrono>

class SQLiteDBConnection
{
public:
    SQLiteDBConnection(const std::string& dbFile) : m_db(nullptr)
    {
        if(sqlite3_open(dbFile.c_str(), &m_db) != SQLITE_OK) //如果 sqlite3_open 成功，它会将数据库句柄存放在 db 中
        {
            std::cerr << "Failed to open database: " << sqlite3_errmsg(m_db) << std::endl;
            m_db = nullptr;
        }
    }
    
    ~SQLiteDBConnection()
    {
        if(m_db)
        {
            sqlite3_close(m_db);
        }
    }

    bool isConnected() const
    {
        return m_db != nullptr;
    }

    bool executeQuery(const std::string& query)
    {
        //std::unique_lock<std::mutex> lock(m_sqlMutex);
        if(!isConnected())
        {
            std::cerr << "Not connected to database." << std::endl;
            return false;
        }

        char* errMsg = nullptr;
        if(sqlite3_exec(m_db, query.c_str(), nullptr, nullptr, &errMsg) != SQLITE_OK) // 执行sql语句
        { 
            std::cerr << "SQL error: " << errMsg << std::endl;
            sqlite3_free(errMsg);//sqlite3_free 释放由 sqlite3_exec 分配的内存
            return false; 
        }
        std::cout << "successful \n" << std::endl;
        return true;
    }

private:
    sqlite3* m_db;
    std::mutex m_sqlMutex;
};

class ConnectionPool
{
public:
    ConnectionPool(const std::string& dbFile, size_t poolSize) : m_dbFile(dbFile), m_poolSize(poolSize)
    {
        initializePool();
    }

    ~ConnectionPool()
    {
        shutdownPool();       
    }

    std::shared_ptr<SQLiteDBConnection> getConnection()
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        while(m_connections.empty()) //可能存在虚假唤醒，所以再次判断
        {
            m_condVar.wait(lock);
        }
        //m_condVar.wait(lock, [this](){ return !m_connections.empty();}); //让线程在满足之前处于阻塞状态
        auto connection = m_connections.front();
        std::cout << "get connection successful \n" << std::endl;
        m_connections.pop();
        return connection;
    }

    void releaseConnection(std::shared_ptr<SQLiteDBConnection> connection)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_connections.push(connection);
        m_condVar.notify_one();  //唤醒一个等待线程 ，conVar.wait()
    }

private:
    void initializePool()
    {
        for(size_t i = 0; i < m_poolSize; ++i)
        {
            auto connection = std::make_shared<SQLiteDBConnection>(m_dbFile);
            if(connection->isConnected())
            {
                m_connections.push(connection);
            }
            else
            {
                std::cerr << "Failed to create connection " << i << std::endl; 
            }
        }
    }

    void shutdownPool()
    {
        while(!m_connections.empty())
        {
            auto connection = m_connections.front();
            m_connections.pop();
        }
    }

private:
    std::string m_dbFile;
    size_t m_poolSize;
    std::queue<std::shared_ptr<SQLiteDBConnection>> m_connections;
    std::mutex m_mutex;
    std::condition_variable m_condVar;
};

void worker(ConnectionPool& pool, int workerId, std::mutex&writeMutex)
{
    auto connection = pool.getConnection();

    std::string query = "INSERT INTO test_table (worker_id, data) VALUES (" + std::to_string(workerId) + ", 'data_" + std::to_string(workerId) + "');";
    {
        std::lock_guard<std::mutex> lock(writeMutex);
        connection->executeQuery(query);
    }
    pool.releaseConnection(connection);
}

int main()
{
    const std::string dbFile = "test.db";
    const size_t poolSize = 5;
    const size_t numWorkers = 10;

    ConnectionPool pool(dbFile, poolSize);

    {
        auto connection = pool.getConnection();
        connection->executeQuery("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, worker_id INTEGER, data TEXT);");
        pool.releaseConnection(connection);
    }   

    std::vector<std::thread> workers;
    std::mutex writeMutex;  
    for(size_t i=0; i<numWorkers; ++i)
    {
       workers.emplace_back(worker, std::ref(pool), i,std::ref(writeMutex));  
       // 使用 std::ref 是为了避免复制 pool 对象，因为 std::thread 的构造函数只接受可移动或可复制的参数。通过传递引用，我们可以确保每个线程都能访问同一个 pool 对象。
    }

    for(auto& worker : workers)
    {
        worker.join();
    }

    return 0;
}
const express = require('express');
const { Client } = require('cassandra-driver');
const cors = require('cors');
const multer = require('multer');
const bodyParser = require('body-parser');
const axios = require('axios');
const { createProxyMiddleware } = require('http-proxy-middleware');
const axiosRetry = require('axios-retry');
const https = require('https'); 
const { ApolloServer, gql } = require('apollo-server');

const app = express();
const port = 3000;

app.use(cors());
app.use(bodyParser.json({limit: '1000mb'}));

const storage = multer.memoryStorage();
const upload = multer({ storage: storage, limits: { fileSize: 100000000000 } });
const neo4j = require("neo4j-driver");
const router = express.Router();

const dotenv = require('dotenv');
dotenv.config();

const client = new Client({
    cloud: { secureConnectBundle: './extras/secure-connect-contemporaryproject.zip' },
    credentials: {
      username: 'pXFMlrqHCMYjZtHHGWWSmMsi',
      password: 'F8UOtYelFa1AsBWo,9O7ia_2M4e7CBb3,RNM9tYshO+zEGh-Ykoz4or5xA27Q_73Ctkxb.76TFNkYRv18wo_D_EWF2o0ZS2EqmZHo,.59YBZkHgTlmEzc,N1PSTH3r+y'
    },
    keyspace: 'tabular'
});


//Tabular
async function connectToCassandra() {
    try {
      await client.connect();
      console.log('Connected to Cassandra');
    } catch (error) {
      console.error('Error connecting to Cassandra:', error);
    }
  }
  
  connectToCassandra();

  
  app.get('/api/get-student', async (req, res) => {
    try {
      const result = await client.execute('SELECT * FROM student_info');
      console.log(result.rows);
      const data = res.json(result.rows);

    } catch (error) {
      console.error('Error executing Cassandra query: ', error);
      res.status(500).json({ error: 'Internal Server Error' });
    }
  });

  app.post('/api/get-student/id', async (req, res) => {
    try {
      const {id} = req.body;
      const query = 'SELECT * FROM student_info WHERE studid = ?';
      const result = await client.execute(query, [id], { consistency: 1 });
      const data = res.json(result.rows);
    }catch(error) {
      console.log('Error executing Cassandra query: ', error);
      res.status(500).json({ error: 'Internal Server Error' });
    }
    
  });

  app.post('/api/login-student', async(req, res) => {
    try {
      const {username, password} = req.body;
      const query = 'SELECT * FROM student_info WHERE stud_username = ? AND stud_password = ? ALLOW FILTERING';
      const result = await client.execute(query, [username, password], {consistency: 1});



      if(result.rows[0] != undefined) {
        res.status(200).json({ success: true });
      }else {
        res.status(401).json({ success: false });
      }
    } catch(error) {
      console.error('Exit with a code of ',error);
      res.status(500).json({ error: 'Internal Server Error' });
    }
  });

  app.post('/api/insert-student', upload.single('image'), async(req, res) => {
    try {
        const { studid, stud_fname, stud_lname, stud_password, stud_username } = req.body;
        const imageBuffer = req.file.buffer;

        const decodeImage = Buffer.from(imageBuffer, 'base64');

        const query = 'INSERT INTO student_info(studid, stud_fname, stud_lname, stud_password, stud_username, image) VALUES (?, ?, ?, ?, ?, ?)';
        await client.execute(query, [studid, stud_fname, stud_lname, stud_password, stud_username,  imageBuffer]);

        res.status(201).json({ message: 'Data entry successful'});

    }catch(error) {
        console.error(error);
    }
  });

  //Document Datastax
  app.use('/api/products', async(req, res) => {
    try {
      const response = await axios.get('https://47fde4cd-06c7-4521-9be0-57f92bbe8786-us-east1.apps.astra.datastax.com/api/rest/v2/namespaces/document/collections/product_list?page-size=20', {
        headers: {
          'X-Cassandra-Token': 'AstraCS:pXFMlrqHCMYjZtHHGWWSmMsi:0714a45613886d9a770293b323dc0de575633c66d89034c5f2d954d98c4dd74a'
        }
      });
      res.json(response.data);
    }catch(error) {
      console.error(error);
    }
  });

  app.use('/api/add-products', async(req, res) => {
    const requestData = req.body;
    try {
      const response = await axios.post('https://47fde4cd-06c7-4521-9be0-57f92bbe8786-us-east1.apps.astra.datastax.com/api/rest/v2/namespaces/document/collections/product_list', 
      requestData,
      {
        headers: {
          'Content-Type': 'application/json',
          'X-Cassandra-Token': 'AstraCS:pXFMlrqHCMYjZtHHGWWSmMsi:0714a45613886d9a770293b323dc0de575633c66d89034c5f2d954d98c4dd74a'
        }
      });
      res.json(response.data);
    }catch(error) {
      console.error(error);
    }
  });

  //Graphql Server / KeyValue
  app.use('/api/orders', async(req, res) => {
    const id = req.body;

    const query = `query oneOrder {
      orders(value: { student_id: "${Object.values(id)}" }) {
        values {
          order_id
          product_name
          purchase_date
        }
      }
    }`;

    if (!query) {
      return res.status(400).json({ error: 'Missing GraphQL query in the request body' });
    }
    const response = await axios.post('https://47fde4cd-06c7-4521-9be0-57f92bbe8786-us-east1.apps.astra.datastax.com/api/graphql/keyvalue',
        {
          query
        },
      {
        headers: {
          'Content-Type': 'application/json',
          'X-Cassandra-Token': 'AstraCS:pXFMlrqHCMYjZtHHGWWSmMsi:0714a45613886d9a770293b323dc0de575633c66d89034c5f2d954d98c4dd74a'
        }
      }

    );
    res.json(response.data);

  });

  //Neo4j
  const driver = neo4j.driver(process.env.NEO4J_URI, neo4j.auth.basic(process.env.NEO4J_USERNAME, process.env.NEO4J_PASSWORD));

  app.get('/api/admin-dashboard', async (req, res) => {
    const session = driver.session();

    try {
      const result = await session.run('MATCH p=()-[:Ordered_Items]->() RETURN p LIMIT 25');
      // const nodes = result.records.map(record => record.get('p').segments.map(segment => segment.relationship.properties));
      const nodes = result.records.map(record => record.get('p').segments.map(segment => {
        return {
          startNode: segment.start.properties,
          relationship: segment.relationship.properties,
          endNode: segment.end.properties
        };
      }));
      res.json(nodes);
    } catch (error) {
      console.error('Error executing Neo4j query:', error);
      res.status(500).json({ error: 'Internal Server Error' });
    } finally {
      await session.close();
    }
  });

  app.get('/api/admin-students', async (req, res) => {
    const session = driver.session();

    try {
      const result = await session.run('MATCH (n:Students) RETURN n LIMIT 25;');
      // const nodes = result.records.map(record => record.get('p').segments.map(segment => segment.relationship.properties));
      const nodes = result.records.map(record => record.get('n').properties);
      res.json(nodes);
    } catch (error) {
      console.error('Error executing Neo4j query:', error);
      res.status(500).json({ error: 'Internal Server Error' });
    } finally {
      await session.close();
    }
  });

  // Start the server
  app.listen(port, () => {
    console.log(`Server listening at http://localhost:${port}`);
  });
const axios = require('axios');

const instance = axios.create({
  timeout: 30000, // 30 วินาที
  httpsAgent: new (require('https').Agent)({ rejectUnauthorized: false }),
});

async function getSubDistrictById(id) {
  try {
    const response = await instance.get(`https://your-api-url/${id}`);
    return response.data;
  } catch (error) {
    console.error('Error:', error.message);
    throw error;
  }
}

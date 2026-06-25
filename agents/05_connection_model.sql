-- ===========================================================================
-- 05_connection_model.sql   (run FIRST in the agents/ sequence)
-- ===========================================================================
-- Creates the LLM connection + model the agents reason with. Pick ONE provider
-- block below, substitute <PLACEHOLDERS>, and run it. Keep the model name
-- `alert_llm` so the agent files don't need edits.
--
-- VERIFY at build time (Streaming Agents is Open Preview): exact property keys
-- (`bedrock.model_id` vs `bedrock.model_version`; azureopenai deployment vs
-- model_version) on the CREATE MODEL reference page for your chosen model.
-- Docs: https://docs.confluent.io/cloud/current/ai/ai-model-inference.html
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- OPTION A — AWS Bedrock hosting Anthropic Claude  (keeps Claude as the model)
-- ---------------------------------------------------------------------------
CREATE CONNECTION bedrock_connection WITH (
  'type'           = 'bedrock',
  'endpoint'       = 'https://bedrock-runtime.<REGION>.amazonaws.com/model/<MODEL_ID>/invoke',
  'aws-access-key' = '<AWS_ACCESS_KEY>',
  'aws-secret-key' = '<AWS_SECRET_KEY>'
  -- ,'aws-session-token' = '<AWS_SESSION_TOKEN>'   -- only for temporary creds
);

CREATE MODEL alert_llm
INPUT  (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider'           = 'bedrock',
  'task'               = 'text_generation',
  'bedrock.connection' = 'bedrock_connection',
  'bedrock.model_id'   = 'anthropic.claude-3-5-sonnet-20240620-v1:0'  -- verify exact id
);

-- ---------------------------------------------------------------------------
-- OPTION B — Azure OpenAI  (uncomment to use instead of Option A)
-- ---------------------------------------------------------------------------
-- CREATE CONNECTION azure_openai_connection WITH (
--   'type'     = 'azureopenai',
--   'endpoint' = 'https://<RESOURCE>.openai.azure.com/openai/deployments/<DEPLOYMENT>/chat/completions?api-version=<API_VERSION>',
--   'api-key'  = '<AZURE_OPENAI_API_KEY>'
-- );
--
-- CREATE MODEL alert_llm
-- INPUT  (prompt STRING)
-- OUTPUT (response STRING)
-- WITH (
--   'provider'               = 'azureopenai',
--   'task'                   = 'text_generation',
--   'azureopenai.connection' = 'azure_openai_connection'
-- );

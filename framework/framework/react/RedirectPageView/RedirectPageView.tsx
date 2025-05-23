/*
 * Copyright 2024 Harness, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from 'react'
import { useEffect } from 'react'
import { useHistory } from 'react-router-dom'

import { useRoutes } from '@ar/hooks'
import type { RedirectPageQueryParams } from '@ar/routes/types'

import { useQueryParams } from 'hooks/useQueryParams'

export default function RedirectPageView() {
  const { registryId, artifactId, versionId, versionDetailsTab } = useQueryParams<RedirectPageQueryParams>()

  const history = useHistory()

  const routes = useRoutes()

  const getRedirectLink = async () => {
    if (registryId && artifactId && versionId && versionDetailsTab) {
      return routes.toARVersionDetailsTab({
        repositoryIdentifier: registryId,
        artifactIdentifier: artifactId,
        versionIdentifier: versionId,
        versionTab: versionDetailsTab
      })
    }
    if (registryId && artifactId && versionId) {
      return routes.toARVersionDetails({
        repositoryIdentifier: registryId,
        artifactIdentifier: artifactId,
        versionIdentifier: versionId
      })
    }
    if (registryId && artifactId) {
      return routes.toARArtifactDetails({
        repositoryIdentifier: registryId,
        artifactIdentifier: artifactId
      })
    }
    if (registryId) {
      return routes.toARRepositoryDetails({
        repositoryIdentifier: registryId
      })
    }
    return routes.toARRepositories()
  }

  const init = async () => {
    const url = await getRedirectLink()
    history.replace(url)
  }

  useEffect(() => {
    init()
  }, [])

  return <></>
}

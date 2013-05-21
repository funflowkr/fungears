# FunGears

## Note

  - Consistent Hash
    - SimpleTable, ElastiCache에 저장할 때 Consistent hash 사용
  - ElastiCache가 ec2 내부에서만 접근가능. 로컬 테스트 불가.

## TODO

  - 결제를 위한 정보 쌓기

## Ranking Service

### Installation

  - sudo yum install libevent-devel
  - sudo yum install python-devel
  - sudo pip install -r ranking/requirements.txt
  - make ranking/config.py from ranking/confg.py.template

### API

모든 요청은 기본적으로 JSON으로 한다

  - GET /game/(gameId)
    - 해당 gameId의 게임에 대한 정보를 얻음
    - Return value: {"id":gameId, "base":리셋 기준 시간, "interval":리셋 간격}
  - POST /game/(gameId)
    - 새로운 게임 생성 / 리셋 기준 시간, 간격 재설정
    - Parameters
      - base: 리셋의 기준이 되는 시간 (1970년 1월 1일 0시 0분 0초 기준, 0초 단위)
  	  - interval: 리셋 간격 (초 단위)
	    - secret (옵션): 게임 생성을 제외한 API를 호출할 때 필요. 생성시 리턴됨.
    - Return value
      - 새로운 게임 생성에 성공한 경우

          {  
            "id":gameId,  
            "base":리셋 기준 시간,  
            "interval":리셋 간격,  
            "secret":API 호출을 위한 secret  
          }

      - 해당 gameId가 이미 존재하는 경우. secret값의 일치 여부에 따라 갱신 여부가 결정됨.

          {  
            "id":gameId,  
            "base":리셋 기준 시간,  
            "interval":리셋 간격  
          }
  
  - POST /update_friends/(gameId)/(userId)
    - 해당 유저의 친구 목록을 설정하는 함수
    - Paramter
      - secret: gameId에 해당하는 secret
      - friends: 친구들의 userId를 배렬로 지정. (예: [1,2,3])
    - Return value

        {  
          success: 성공 여부에 따라 true 또는 false  
        }

  - GET /friend_scores/(gameId)/(userId)
    - 해당 유저의 본인 점수 및 친구 점수를 읽어옴
    - Paramter
      - secret: gameId에 해당하는 secret
    - Return value
      - [[id1, score1], [id2, score2], ...]
      - 0번 유저가 1, 2, 3번 유저를 친구로 가지고 있는 경우의 리턴 값은 [[0,0],[1,0],[2,0],[3,0]]의 형태가 된다.
	  - 점수가 큰 유저부터 정렬되서 리턴됨.

  - POST /update_score/(gameId)/(userId)/(score)
    - 해당 유저의 점수를 갱신
    - Paramter
      - secret: gameId에 해당하는 secret
	  - forced(옵션): true로 주면 강제로 점수 설정 가능
    - Return value

        {  
          "score":새 점수,  
          "updated":갱신 여부에 따라 true 또는 false  
        }

  - GET /ranking_from/(gameId)/(userId)
    - 다른 유저 기준으로 등수를 얻음. (다른 유저의 친구 리스트 기준)
    - Parameter
      - secret: gameId에 해당하는 secret
      - view_from: 기준이 될 유저의 userId를 배열로 지정. (예: [1,2,3])
    - Return value

        [[id1, rank1], [id2, rank2], ...]

      - from에 주어진 각 id별로 등수를 얻어 배열에 담아 리턴함.

  - GET /debug_get_scores_and_ranking/(gameId)/(userId)
    - friend_scores와 ranking_from을 합친 기능
	- 부하가 클 것으로 예상됨
    - Parameter
      - secret: gameId에 해당하는 secret
    - Return value

        [[id1, score1, rank1], [id2, score2, rank2], ...]

      - 친구와 본인을 포함해서 점수와 해당 친구 기준의 랭킹 값을 하나로 합쳐서 리턴.

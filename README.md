# FunGears

## TODO
  - 점수 갱신할 때도 key가 맞을 때만 하게

## Ranking Service

  - GET /game/(gameId)
    - 해당 gameId의 게임에 대한 정보를 얻음
    - Return value: {"id":gameId, "base":리셋 기준 시간, "interval":리셋 간격}
  - POST /game/(gameId)
    - 새로운 게임 생성 / 리셋 기준 시간, 간격 재설정
    - Parameters
      - base: 리셋의 기준이 되는 시간
  	  - interval: 리셋 간격
	    - key (옵션): 이미 생성된 게임의 값을 변경하려할 때 필요. 생성시 리턴됨.
    - Return value
      - 새로운 게임 생성에 성공한 경우

          {  
            "id":gameId,  
            "base":리셋 기준 시간,  
            "interval":리셋 간격,  
            "key":게임 정보 설정을 위한 key  
          }

      - 해당 gameId가 이미 존재하는 경우. key값의 일치 여부에 따라 갱신 여부가 결정됨.

          {  
            "id":gameId,  
            "base":리셋 기준 시간,  
            "interval":리셋 간격  
          }
  
  - POST /update_friends/(gameId)/(userId)
    - Paramter
  - GET /friend_scores/(gameId)/(userId)
    - 해당 유저의 친구 점수를 읽어옴
    - 0번 유저가 1, 2, 3번 유저를 친구로 가지고 있는 경우의 리턴 값은 [[0,0],[1,0],[2,0],[3,0]]의 형태가 된다.

  - POST /update_score/(gameId)/(userId)/(score)
    - 해당 유저의 점수를 갱신
    - Return value

        {  
          "score":새 점수,  
          "updated":갱신 여부에 따라 true 또는 false  
        }